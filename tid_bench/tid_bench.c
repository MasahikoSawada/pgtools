/* -------------------------------------------------------------------------
 *
 * tid_bench.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/index.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "lib/integerset.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "lvdeadtuple.h"

PG_MODULE_MAGIC;

#define MAX_TUPLES_PER_PAGE  MaxHeapTuplesPerPage
#define PAGES_PER_CHUNK  (BLCKSZ / 32)

/* We use BITS_PER_BITMAPWORD and typedef bitmapword from nodes/bitmapset.h */

#define WORDNUM(x)  ((x) / BITS_PER_BITMAPWORD)
#define BITNUM(x)   ((x) % BITS_PER_BITMAPWORD)

/* number of active words for an exact page: */
#define WORDS_PER_PAGE  ((MAX_TUPLES_PER_PAGE - 1) / BITS_PER_BITMAPWORD + 1)
/* number of active words for a lossy chunk: */
#define WORDS_PER_CHUNK  ((PAGES_PER_CHUNK - 1) / BITS_PER_BITMAPWORD + 1)
typedef struct PagetableEntry
{
    BlockNumber blockno;        /* page number (hashtable key) */
    char        status;         /* hash entry status */
    bool        ischunk;        /* T = lossy storage, F = exact */
    bool        recheck;        /* should the tuples be rechecked? */
    bitmapword  words[Max(WORDS_PER_PAGE, WORDS_PER_CHUNK)];
} PagetableEntry;

typedef struct DeadTupleInfo
{
	uint64 nitems;
	BlockNumber minblk;
	BlockNumber maxblk;
	OffsetNumber maxoff;
} DeadTupleInfo;
typedef struct DeadTuplesArray
{
	DeadTupleInfo dtinfo;
	ItemPointer itemptrs;
} DeadTuplesArray;

typedef struct LVTestType LVTestType;
typedef struct LVTestType
{
	DeadTupleInfo dtinfo;

	char *name;
	void *private;

	MemoryContext mcxt;

	void (*init_fn) (struct LVTestType *lvtt, uint64 nitems);
	void (*fini_fn) (struct LVTestType *lvtt);
	void (*attach_fn) (struct LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
					   BlockNumber maxblk, OffsetNumber maxoff);
	bool (*reaped_fn) (struct LVTestType *lvtt, ItemPointer itemptr);
	Size (*show_mem_usage_fn) (struct LVTestType *lvtt);
} LVTestType;

/* Simulated index tuples always uses an simple array */
static DeadTuplesArray *IndexTids_cache = NULL;
static DeadTuplesArray *DeadTuples_orig = NULL;

PG_FUNCTION_INFO_V1(prepare_index_tuples);
PG_FUNCTION_INFO_V1(prepare_dead_tuples);
PG_FUNCTION_INFO_V1(prepare_index_tuples2);
PG_FUNCTION_INFO_V1(prepare_dead_tuples2);
PG_FUNCTION_INFO_V1(prepare_dead_tuples2_packed);
PG_FUNCTION_INFO_V1(attach_dead_tuples);
PG_FUNCTION_INFO_V1(tid_bench);
PG_FUNCTION_INFO_V1(test_generate_tid);

/*
PG_FUNCTION_INFO_V1(tbm_test);
PG_FUNCTION_INFO_V1(dtstore_test);
PG_FUNCTION_INFO_V1(itereate_bench);
*/

/* array */
static void array_init(LVTestType *lvtt, uint64 nitems);
static void array_fini(LVTestType *lvtt);
static void array_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool array_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size array_show_mem_usage(LVTestType *lvtt);

/* tbm */
static void tbm_init(LVTestType *lvtt, uint64 nitems);
static void tbm_fini(LVTestType *lvtt);
static void tbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool tbm_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size tbm_show_mem_usage(LVTestType *lvtt);

/* intset */
static void intset_init(LVTestType *lvtt, uint64 nitems);
static void intset_fini(LVTestType *lvtt);
static void intset_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool intset_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size intset_show_mem_usage(LVTestType *lvtt);

/* dtstore */
static void dtstore_init(LVTestType *lvtt, uint64 nitems);
static void dtstore_fini(LVTestType *lvtt);
static void dtstore_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool dtstore_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size dtstore_show_mem_usage(LVTestType *lvtt);


/* Misc functions */
static void generate_index_tuples(uint64 nitems, BlockNumber minblk,
								  BlockNumber maxblk, OffsetNumber maxoff);
static void generate_dead_tuples_orig(uint64 nitems, BlockNumber minblk,
									  BlockNumber maxblk, OffsetNumber maxoff);
static void generate_random_itemptrs(uint64 nitems,
									 BlockNumber minblk, BlockNumber maxblk,
									 OffsetNumber maxoff, ItemPointer itemptrs_out);
static void attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk, BlockNumber maxblk,
				   OffsetNumber maxoff);
static int vac_cmp_itemptr(const void *left, const void *right);
static void load_dtstore(DeadTupleStore *dtstore, ItemPointerData *itemptrs, int nitems);

#define DECLARE_SUBJECT(n) \
	{ \
		.dtinfo = {0}, \
		.name = #n, \
		.init_fn = n##_init, \
		.attach_fn = n##_attach, \
		.reaped_fn = n##_reaped, \
		.show_mem_usage_fn = n##_show_mem_usage, \
			}

#define TEST_SUBJECT_TYPES 4
static LVTestType LVTestSubjects[TEST_SUBJECT_TYPES] =
{
	DECLARE_SUBJECT(array),
	DECLARE_SUBJECT(tbm),
	DECLARE_SUBJECT(intset),
	DECLARE_SUBJECT(dtstore),
};

static bool
is_cached(DeadTupleInfo *info, uint64 nitems, BlockNumber minblk,
		  BlockNumber maxblk, OffsetNumber maxoff)
{
	return (info &&
			info->nitems == nitems &&
			info->minblk == minblk &&
			info->maxblk == maxblk &&
			info->maxoff == maxoff);
}

static void
update_info(DeadTupleInfo *info, uint64 nitems, BlockNumber minblk,
		  BlockNumber maxblk, OffsetNumber maxoff)
{
	info->nitems = nitems;
	info->minblk = minblk;
	info->maxblk = maxblk;
	info->maxoff = maxoff;
}

static void
generate_index_tuples(uint64 nitems, BlockNumber minblk, BlockNumber maxblk,
					 OffsetNumber maxoff)
{
	/* Return the cached set if the same request comes */
	if (is_cached((DeadTupleInfo *) IndexTids_cache, nitems, minblk, maxblk, maxoff))
		return;

	if (!IndexTids_cache)
		IndexTids_cache = MemoryContextAllocHuge(TopMemoryContext,
											  sizeof(DeadTuplesArray));
	else
		pfree(IndexTids_cache->itemptrs);

	/* update cache information */
	update_info((DeadTupleInfo *) IndexTids_cache, nitems, minblk, maxblk, maxoff);
	IndexTids_cache->itemptrs = (ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
																  sizeof(ItemPointerData) * nitems);

	generate_random_itemptrs(nitems, minblk, maxblk, maxoff,
							 IndexTids_cache->itemptrs);
}

static void
generate_dead_tuples_orig(uint64 nitems, BlockNumber minblk, BlockNumber maxblk,
						  OffsetNumber maxoff)
{
	/* Return the cached set if the same request comes */
	if (is_cached((DeadTupleInfo *) DeadTuples_orig, nitems, minblk, maxblk, maxoff))
		return;

	if (!DeadTuples_orig)
		DeadTuples_orig = MemoryContextAllocHuge(TopMemoryContext,
												 sizeof(DeadTuplesArray));
	else
		pfree(DeadTuples_orig->itemptrs);

	/* update cache information */
	update_info((DeadTupleInfo *) DeadTuples_orig, nitems, minblk, maxblk, maxoff);
	DeadTuples_orig->itemptrs = (ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
																  sizeof(ItemPointerData) * nitems);

	generate_random_itemptrs(nitems, minblk, maxblk, maxoff,
							 DeadTuples_orig->itemptrs);
}

static void
generate_random_itemptrs(uint64 nitems,
						 BlockNumber minblk, BlockNumber maxblk,
						 OffsetNumber maxoff, ItemPointer itemptrs_out)
{
	int nblocks;
	int ntids_available;
	int interval;
	BlockNumber blk = minblk;
	OffsetNumber off = FirstOffsetNumber;


	nblocks = maxblk - minblk;
	ntids_available = nblocks * (maxoff - 1);
	if (ntids_available < nitems)
		interval = 1;
	else
		interval = ntids_available / nitems;

	elog(NOTICE, "generating %lu itemptrs from %u to %u blk, each having at most %u offset, with interval %d",
		 nitems, minblk, maxblk, maxoff, interval);

	for (int i = 0; i < nitems; i++)
	{
		ItemPointerSetBlockNumber(&(itemptrs_out[i]), blk);
		ItemPointerSetOffsetNumber(&(itemptrs_out[i]), off);

		off += interval;

		while (off > maxoff)
		{
			off -= maxoff;
			blk++;
		}

		if (off < FirstOffsetNumber)
			off += maxoff;
	}
}

/****************************************************************************************/
/* ---------- ARRAY ---------- */
static void array_init(LVTestType *lvtt, uint64 nitems)
{
	lvtt->private = (ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
														 sizeof(ItemPointerData) * nitems);
}
static void array_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		pfree(lvtt->private);
}
static void array_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff)
{
	ItemPointer itemptrs = (ItemPointer) lvtt->private;

	for (int i = 0; i < nitems; i++)
	{
		ItemPointerSetBlockNumber(&(itemptrs[i]),
								  ItemPointerGetBlockNumber(&(DeadTuples_orig->itemptrs[i])));
		ItemPointerSetOffsetNumber(&(itemptrs[i]),
								   ItemPointerGetOffsetNumber(&(DeadTuples_orig->itemptrs[i])));
	}
}
static inline int
vac_cmp_itemptr(const void *left, const void *right)
{
    BlockNumber lblk,
                rblk;
    OffsetNumber loff,
                roff;

    lblk = ItemPointerGetBlockNumber((ItemPointer) left);
    rblk = ItemPointerGetBlockNumber((ItemPointer) right);

    if (lblk < rblk)
        return -1;
    if (lblk > rblk)
        return 1;

    loff = ItemPointerGetOffsetNumber((ItemPointer) left);
    roff = ItemPointerGetOffsetNumber((ItemPointer) right);

    if (loff < roff)
        return -1;
    if (loff > roff)
        return 1;

    return 0;
}
static bool array_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	ItemPointer dead_tuples = (ItemPointer) lvtt->private;
	int64		litem,
				ritem,
				item;
	ItemPointer res;

	litem = itemptr_encode(&(dead_tuples[0]));
	ritem = itemptr_encode(&(dead_tuples[lvtt->dtinfo.nitems - 1]));
	item = itemptr_encode(itemptr);

	/*
	 * Doing a simple bound check before bsearch() is useful to avoid the
	 * extra cost of bsearch(), especially if dead tuples on the heap are
	 * concentrated in a certain range.  Since this function is called for
	 * every index tuple, it pays to be really fast.
	 */
	if (item < litem || item > ritem)
		return false;

	res = (ItemPointer) bsearch((void *) itemptr,
								(void *) dead_tuples,
								lvtt->dtinfo.nitems,
								sizeof(ItemPointerData),
								vac_cmp_itemptr);

	return (res != NULL);
}
static uint64
array_show_mem_usage(LVTestType *lvtt)
{
	return sizeof(ItemPointerData) * lvtt->dtinfo.nitems;
}

/* ---------- TBM ---------- */
static void
tbm_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "array bench",
									   ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) tbm_create(sizeof(ItemPointerData) * nitems, NULL);
	MemoryContextSwitchTo(old_ctx);
}
static void
tbm_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		tbm_free(lvtt->private);
}
static void
tbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
		   BlockNumber maxblk, OffsetNumber maxoff)
{
	TIDBitmap *tbm = (TIDBitmap *) lvtt->private;

	tbm_add_tuples(tbm, DeadTuples_orig->itemptrs, nitems, false);
}
static bool
tbm_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	//return tbm_is_member((TIDBitmap *) lvtt->private, itemptr);
	return true;
}
static Size
tbm_show_mem_usage(LVTestType *lvtt)
{
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

/* ---------- INTSET ---------- */
static void
intset_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "intset bench",
									   ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) intset_create();
	MemoryContextSwitchTo(old_ctx);
}
static void intset_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		pfree(lvtt->private);
}
static void
intset_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			  BlockNumber maxblk, OffsetNumber maxoff)
{
	for (int i = 0; i < lvtt->dtinfo.nitems; i++)
		intset_add_member((IntegerSet *) lvtt->private,
						  itemptr_encode(&DeadTuples_orig->itemptrs[i]));

}
static bool
intset_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	return intset_is_member((IntegerSet *) lvtt->private, itemptr_encode(itemptr));
}
static uint64
intset_show_mem_usage(LVTestType *lvtt)
{
	return intset_memory_usage((IntegerSet *) lvtt->private);
}

/* ---------- DTSTORE ---------- */
static void
dtstore_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "dtstore bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) dtstore_create();
	MemoryContextSwitchTo(old_ctx);
}
static void
dtstore_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		dtstore_free((DeadTupleStore *) lvtt->private);
}
static void
dtstore_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			   BlockNumber maxblk, OffsetNumber maxoff)
{
	load_dtstore((DeadTupleStore *) lvtt->private,
				 DeadTuples_orig->itemptrs,
				 DeadTuples_orig->dtinfo.nitems);
}
static bool
dtstore_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	return dtstore_lookup((DeadTupleStore *) lvtt->private, itemptr);
}
static uint64
dtstore_show_mem_usage(LVTestType *lvtt)
{
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

static void
load_dtstore(DeadTupleStore *dtstore, ItemPointerData *itemptrs, int nitems)
{
	BlockNumber curblkno = InvalidBlockNumber;
	OffsetNumber offs[1024];
	int noffs = 0;

	for (int i = 0; i < nitems; i++)
	{
		ItemPointer tid = &(itemptrs[i]);
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);

		if (curblkno != InvalidBlockNumber &&
			curblkno != blkno)
		{
			dtstore_add_tuples(dtstore,
							   curblkno, offs, noffs);
			curblkno = blkno;
			noffs = 0;
		}

		curblkno = blkno;
		offs[noffs++] = ItemPointerGetOffsetNumber(tid);
	}

	dtstore_add_tuples(dtstore, curblkno, offs, noffs);
}

static void
attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk, BlockNumber maxblk,
	   OffsetNumber maxoff)
{
	MemoryContext old_ctx;

	if (!DeadTuples_orig || DeadTuples_orig->dtinfo.nitems == 0)
		elog(ERROR, "must prepare dead tuple tids by ");

	/* Return the cached set if the same request comes */
	if (is_cached((DeadTupleInfo *) &(lvtt->dtinfo), nitems, minblk, maxblk, maxoff))
		return;

	/* (re) initialize */
	if (!lvtt->private)
		lvtt->init_fn(lvtt, nitems);
	else
	{
		lvtt->fini_fn(lvtt);
		lvtt->init_fn(lvtt, nitems);
	}

	/* update cache information */
	update_info((DeadTupleInfo *) &(lvtt->dtinfo), nitems, minblk, maxblk, maxoff);

	if (lvtt->mcxt)
		old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

	lvtt->attach_fn(lvtt, nitems, minblk, maxblk, maxoff);

	if (lvtt->mcxt)
		MemoryContextSwitchTo(old_ctx);
}

static void
bench(LVTestType *lvtt)
{
	int matched = 0;
	MemoryContext old_ctx;

	if (!lvtt->private)
		elog(ERROR, "%s dead tuples are not preapred", lvtt->name);

	if (lvtt->mcxt)
		old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

	for (int i = 0; i < IndexTids_cache->dtinfo.nitems; i++)
	{
		CHECK_FOR_INTERRUPTS();
		if (lvtt->reaped_fn(lvtt, &(IndexTids_cache->itemptrs[i])))
			matched++;
	}

	if (lvtt->mcxt)
		MemoryContextSwitchTo(old_ctx);

	elog(NOTICE, "\"%s\": dead tuples %lu, index tuples %lu, mathed %d, mem %zu",
		 lvtt->name,
		 lvtt->dtinfo.nitems,
		 IndexTids_cache->dtinfo.nitems,
		 matched,
		 lvtt->show_mem_usage_fn(lvtt));
}

/* SQL-callable functions */
Datum
prepare_index_tuples(PG_FUNCTION_ARGS)
{
	uint64 nitems = PG_GETARG_INT64(0);
	BlockNumber minblk = PG_GETARG_INT32(1);
	BlockNumber maxblk = PG_GETARG_INT32(2);
	OffsetNumber maxoff = PG_GETARG_INT32(3);

	generate_index_tuples(nitems, minblk, maxblk, maxoff);

	PG_RETURN_NULL();
}

Datum
prepare_dead_tuples(PG_FUNCTION_ARGS)
{
	uint64 nitems = PG_GETARG_INT64(0);
	BlockNumber minblk = PG_GETARG_INT32(1);
	BlockNumber maxblk = PG_GETARG_INT32(2);
	OffsetNumber maxoff = PG_GETARG_INT32(3);

	generate_dead_tuples_orig(nitems, minblk, maxblk, maxoff);

	PG_RETURN_NULL();
}

Datum
prepare_index_tuples2(PG_FUNCTION_ARGS)
{
	uint64 ntuples = PG_GETARG_INT64(0);
	int tuple_size = PG_GETARG_INT32(1);

	uint64 nitems;
	BlockNumber minblk = 0, maxblk;
	OffsetNumber maxoff;

	maxblk = ((((uint64) ntuples) * (uint64)(tuple_size)) / 8192) * 1.05;
	maxoff = 8192 / tuple_size;
	nitems = ntuples;

	generate_index_tuples(nitems, minblk, maxblk, maxoff);

	PG_RETURN_NULL();
}

Datum
prepare_dead_tuples2(PG_FUNCTION_ARGS)
{
	uint64 ntuples = PG_GETARG_INT64(0);
	int tuple_size = PG_GETARG_INT32(1);
	float8 dt_ratio = PG_GETARG_FLOAT8(2);

	uint64 nitems;
	BlockNumber minblk = 0, maxblk;
	OffsetNumber maxoff;

	maxblk = ((((uint64) ntuples) * (uint64)(tuple_size)) / 8192) * 1.05;
	maxoff = 8192 / tuple_size;
	nitems = (int) (ntuples * dt_ratio);

	generate_dead_tuples_orig(nitems, minblk, maxblk, maxoff);

	PG_RETURN_NULL();
}

Datum
prepare_dead_tuples2_packed(PG_FUNCTION_ARGS)
{
	uint64 ntuples = PG_GETARG_INT64(0);
	int tuple_size = PG_GETARG_INT32(1);
	float8 dt_ratio = PG_GETARG_FLOAT8(2);

	uint64 nitems;
	BlockNumber minblk = 0, maxblk;
	OffsetNumber maxoff;

	nitems = (int) (ntuples * dt_ratio);
	maxblk = (((uint64) nitems) * (uint64)(tuple_size)) / 8192;
	maxoff = 8192 / tuple_size;

	generate_dead_tuples_orig(nitems, minblk, maxblk, maxoff);

	PG_RETURN_NULL();
}

Datum
attach_dead_tuples(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_P(0));

	for (int i = 0; i < TEST_SUBJECT_TYPES; i++)
	{
		LVTestType *lvtt = &(LVTestSubjects[i]);

		if (strcmp(mode, lvtt->name) == 0)
		{
			MemoryContext old_ctx;

			if (lvtt->mcxt)
				old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

			attach(lvtt,
				   DeadTuples_orig->dtinfo.nitems,
				   DeadTuples_orig->dtinfo.minblk,
				   DeadTuples_orig->dtinfo.maxblk,
				   DeadTuples_orig->dtinfo.maxoff);

			if (lvtt->mcxt)
				MemoryContextSwitchTo(old_ctx);

			break;
		}
	}

	PG_RETURN_NULL();
}

Datum
tid_bench(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!IndexTids_cache || !IndexTids_cache->itemptrs)
		elog(ERROR, "index tuples are not preapred");

	for (int i = 0; i < TEST_SUBJECT_TYPES; i++)
	{
		LVTestType *lvtt = &(LVTestSubjects[i]);
		if (strcmp(mode, lvtt->name) == 0)
		{
			bench(lvtt);
			break;
		}
	}

	PG_RETURN_NULL();
}

/*
Datum
itereate_bench(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_P(0));
	int cnt = 0;

	if (strcmp(mode, "array") == 0)
	{
		if (!DeadTuples_array || !DeadTuples_array->itemptrs)
			elog(ERROR, "ARRAY dead tuples are not preapred");

		for (int i = 0; i < DeadTuples_array->dtinfo.nitems; i++)
			cnt++;

	}
	else if (strcmp(mode, "tbm") == 0)
	{
		TBMIterator *iter;
		TBMIterateResult *res;

		if (!DeadTuples_tbm || !DeadTuples_tbm->tbm)
			elog(ERROR, "TBM dead tuples are not preapred");

		iter = tbm_begin_iterate(DeadTuples_tbm->tbm);

		while ((res = tbm_iterate(iter)) != NULL)
		{
			for (int i = 0; i < res->ntuples; i++)
				cnt++;
		}

		tbm_end_iterate(iter);
	}
	else if (strcmp(mode, "intset") == 0)
	{
		uint64 next;
		if (!DeadTuples_intset || !DeadTuples_intset->intset)
			elog(ERROR, "INTSET dead tuples are not preapred");

		intset_begin_iterate(DeadTuples_intset->intset);

		while (intset_iterate_next(DeadTuples_intset->intset, &next))
			cnt++;
	}

	elog(NOTICE, "count %d", cnt);

	PG_RETURN_NULL();
}
*/

Datum
test_generate_tid(PG_FUNCTION_ARGS)
{
	uint64 nitems = PG_GETARG_INT32(0);
	BlockNumber minblk = PG_GETARG_INT32(1);
	BlockNumber maxblk = PG_GETARG_INT32(2);
	OffsetNumber maxoff = PG_GETARG_INT32(3);
	ItemPointer itemptrs = palloc(sizeof(ItemPointer) * nitems);
	StringInfoData buf;

	generate_random_itemptrs(nitems, minblk, maxblk, maxoff, itemptrs);

	initStringInfo(&buf);
	for (int i = 0; i < nitems; i++)
	{
		appendStringInfo(&buf, "(%u %u) ",
						 ItemPointerGetBlockNumber(&itemptrs[i]),
						 ItemPointerGetOffsetNumber(&itemptrs[i]));
	}
	elog(NOTICE, "%s", buf.data);

	PG_RETURN_NULL();
}

/*
Datum
dtstore_test(PG_FUNCTION_ARGS)
{
#define NITEMS_INDEX 1000000000
#define NITEMS_DEAD 200000000

	DeadTupleStore *dtstore = dtstore_create();
	IntegerSet *intset = intset_create();
	ItemPointerData *itemptrs =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * NITEMS_DEAD);
	ItemPointerData *indextups =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * NITEMS_INDEX);
	int matched_intset = 0, matched_dtstore = 0;

	generate_random_itemptrs(NITEMS_INDEX, 0, 12817382, 81, indextups);
	generate_random_itemptrs(NITEMS_DEAD, 0, 12817382, 81, itemptrs);

	elog(NOTICE, "loading to intset...");
	for (int i = 0; i < NITEMS_DEAD; i++)
		intset_add_member(intset, itemptr_encode(&itemptrs[i]));

	elog(NOTICE, "loading to dtstore...");
	load_dtstore(dtstore, itemptrs, NITEMS_DEAD);

	elog(NOTICE, "verifying...");
	for (int i = 0; i < NITEMS_INDEX; i++)
	{
		bool ret1, ret2;

		CHECK_FOR_INTERRUPTS();

		ret1 = tid_reaped_intset(&(indextups[i]), intset);
		ret2 = tid_reaped_dtstore(&(indextups[i]), dtstore);

		if (i % 10000000 == 0)
			elog(NOTICE, "%d done", i);

		if (ret1 != ret2)
		{
			dtstore_dump_blk(dtstore, i);
			elog(ERROR, "failed (%d, %d) : intset %d dtstore %d",
				 ItemPointerGetBlockNumber(&(indextups[i])),
				 ItemPointerGetOffsetNumber(&(indextups[i])),
				 ret1, ret2);
		}

		if (ret1)
			matched_intset++;
		if (ret2)
			matched_dtstore++;
	}

	elog(NOTICE, "matched intset %d dtstore %d",
		 matched_intset,
		 matched_dtstore);

	PG_RETURN_NULL();
}

Datum
tbm_test(PG_FUNCTION_ARGS)
{
	elog(NOTICE, "max entries %lu, sizeof(PagetableEntry) %lu sizeof(Pointer) %lu sizeof(bitmapword) %lu len %d MaxHeapTuplesPerPages %d, sizeof(HeapTupleHeaderData) %lu",
		 tbm_calculate_entries(1024 * 1024 * 64),
		 sizeof(PagetableEntry),
		 sizeof(Pointer),
		 sizeof(bitmapword),
		 Max(WORDS_PER_PAGE, WORDS_PER_CHUNK),
		 MaxHeapTuplesPerPage,
		 sizeof(HeapTupleHeaderData));

	PG_RETURN_NULL();
}
*/
