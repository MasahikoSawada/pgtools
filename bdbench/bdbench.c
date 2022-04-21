/* -------------------------------------------------------------------------
 *
 * bdbench.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <math.h>
#include "catalog/index.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"
#include "lib/integerset.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "common/pg_prng.h"
#include "lib/radixtree.h"

#include "vtbm.h"
#include "rtbm.h"
#include "radix.h"
#include "svtm.h"
//#include "radix_tree.h"

//#define DEBUG_DUMP_MATCHED 1

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
	Size (*mem_usage_fn) (struct LVTestType *lvtt);
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
PG_FUNCTION_INFO_V1(bench);
PG_FUNCTION_INFO_V1(test_generate_tid);
PG_FUNCTION_INFO_V1(rtbm_test);
PG_FUNCTION_INFO_V1(radix_run_tests);
PG_FUNCTION_INFO_V1(prepare);

/*
PG_FUNCTION_INFO_V1(tbm_test);
PG_FUNCTION_INFO_V1(vtbm_test);
PG_FUNCTION_INFO_V1(itereate_bench);
*/

/* array */
static void array_init(LVTestType *lvtt, uint64 nitems);
static void array_fini(LVTestType *lvtt);
static void array_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool array_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size array_mem_usage(LVTestType *lvtt);

/* tbm */
static void tbm_init(LVTestType *lvtt, uint64 nitems);
static void tbm_fini(LVTestType *lvtt);
static void tbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool tbm_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size tbm_mem_usage(LVTestType *lvtt);

/* intset */
static void intset_init(LVTestType *lvtt, uint64 nitems);
static void intset_fini(LVTestType *lvtt);
static void intset_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool intset_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size intset_mem_usage(LVTestType *lvtt);

/* vtbm */
static void vtbm_init(LVTestType *lvtt, uint64 nitems);
static void vtbm_fini(LVTestType *lvtt);
static void vtbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool vtbm_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size vtbm_mem_usage(LVTestType *lvtt);

/* rtbm */
static void rtbm_init(LVTestType *lvtt, uint64 nitems);
static void rtbm_fini(LVTestType *lvtt);
static void rtbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool rtbm_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size rtbm_mem_usage(LVTestType *lvtt);

/* radix */
static void radix_init(LVTestType *lvtt, uint64 nitems);
static void radix_fini(LVTestType *lvtt);
static void radix_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool radix_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size radix_mem_usage(LVTestType *lvtt);
static void radix_load(void *tbm, ItemPointerData *itemptrs, int nitems);

/* svtm */
static void svtm_init(LVTestType *lvtt, uint64 nitems);
static void svtm_fini(LVTestType *lvtt);
static void svtm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
						 BlockNumber maxblk, OffsetNumber maxoff);
static bool svtm_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size svtm_mem_usage(LVTestType *lvtt);
static void svtm_load(SVTm *tbm, ItemPointerData *itemptrs, int nitems);

/* radix_tree */
static void radix_tree_init(LVTestType *lvtt, uint64 nitems);
static void radix_tree_fini(LVTestType *lvtt);
static void radix_tree_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
							  BlockNumber maxblk, OffsetNumber maxoff);
static bool radix_tree_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size radix_tree_mem_usage(LVTestType *lvtt);
static void radix_tree_load(void *tbm, ItemPointerData *itemptrs, int nitems);

/* hash table */
static void hash_init(LVTestType *lvtt, uint64 nitems);
static void hash_fini(LVTestType *lvtt);
static void hash_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
							  BlockNumber maxblk, OffsetNumber maxoff);
static bool hash_reaped(LVTestType *lvtt, ItemPointer itemptr);
static Size hash_mem_usage(LVTestType *lvtt);

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
static void load_vtbm(VTbm *vtbm, ItemPointerData *itemptrs, int nitems);
static void load_rtbm(RTbm *vtbm, ItemPointerData *itemptrs, int nitems);

#define DECLARE_SUBJECT(n) \
	{ \
		.dtinfo = {0}, \
		.name = #n, \
		.init_fn = n##_init, \
		.fini_fn = n##_fini, \
		.attach_fn = n##_attach, \
		.reaped_fn = n##_reaped, \
		.mem_usage_fn = n##_mem_usage, \
			}

#define TEST_SUBJECT_TYPES 9
static LVTestType LVTestSubjects[TEST_SUBJECT_TYPES] =
{
	DECLARE_SUBJECT(array),
	DECLARE_SUBJECT(tbm),
	DECLARE_SUBJECT(intset),
	DECLARE_SUBJECT(vtbm),
	DECLARE_SUBJECT(rtbm),
	DECLARE_SUBJECT(radix),
	DECLARE_SUBJECT(svtm),
	DECLARE_SUBJECT(radix_tree),
	DECLARE_SUBJECT(hash),
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


/* from geqo's init_tour(), geqo_randint() */
static int
shuffle_randrange(pg_prng_state *state, int lower, int upper)
{
	return (int) floor(pg_prng_double(state) * ((upper-lower)+0.999999)) + lower;
}

/* Naive Fisher-Yates implementation*/
static void
shuffle_itemptrs(uint64 nitems, ItemPointer itemptrs)
{
	/* reproducability */
	pg_prng_state state;

	pg_prng_seed(&state, 0);

	for (int i = 0; i < nitems - 1; i++)
	{
		int j = shuffle_randrange(&state, i, nitems - 1);
		ItemPointerData t = itemptrs[j];

		itemptrs[j] = itemptrs[i];
		itemptrs[i] = t;
	}
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
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "array bench",
									   ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (ItemPointer) MemoryContextAllocHuge(lvtt->mcxt,
														 sizeof(ItemPointerData) * nitems);
	MemoryContextSwitchTo(old_ctx);
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
array_mem_usage(LVTestType *lvtt)
{
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

/* ---------- TBM ---------- */
static void
tbm_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "tbm bench",
									   ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) tbm_create(100 * nitems, NULL);
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
	return tbm_is_member((TIDBitmap *) lvtt->private, itemptr);
	//return true;
}
static Size
tbm_mem_usage(LVTestType *lvtt)
{
	//tbm_stats((TIDBitmap *) lvtt->private);
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
intset_mem_usage(LVTestType *lvtt)
{
	return intset_memory_usage((IntegerSet *) lvtt->private);
}

/* ---------- VTBM ---------- */
static void
vtbm_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "vtbm bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) vtbm_create();
	MemoryContextSwitchTo(old_ctx);
}
static void
vtbm_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		vtbm_free((VTbm *) lvtt->private);
}
static void
vtbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			   BlockNumber maxblk, OffsetNumber maxoff)
{
	load_vtbm((VTbm *) lvtt->private,
				 DeadTuples_orig->itemptrs,
				 DeadTuples_orig->dtinfo.nitems);
}
static bool
vtbm_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	return vtbm_lookup((VTbm *) lvtt->private, itemptr);
}
static uint64
vtbm_mem_usage(LVTestType *lvtt)
{
	vtbm_stats((VTbm *) lvtt->private);
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

static void
load_vtbm(VTbm *vtbm, ItemPointerData *itemptrs, int nitems)
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
			vtbm_add_tuples(vtbm,
							   curblkno, offs, noffs);
			curblkno = blkno;
			noffs = 0;
		}

		curblkno = blkno;
		offs[noffs++] = ItemPointerGetOffsetNumber(tid);
	}

	vtbm_add_tuples(vtbm, curblkno, offs, noffs);
}

/* ---------- RTBM ---------- */
static void
rtbm_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "rtbm bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = (void *) rtbm_create();
	MemoryContextSwitchTo(old_ctx);
}
static void
rtbm_fini(LVTestType *lvtt)
{
	if (lvtt->private)
		rtbm_free((RTbm *) lvtt->private);
}
static void
rtbm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			   BlockNumber maxblk, OffsetNumber maxoff)
{
	load_rtbm((RTbm *) lvtt->private,
				   DeadTuples_orig->itemptrs,
				   DeadTuples_orig->dtinfo.nitems);
}
static bool
rtbm_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	return rtbm_lookup((RTbm *) lvtt->private, itemptr);
}
static uint64
rtbm_mem_usage(LVTestType *lvtt)
{
	rtbm_stats((RTbm *) lvtt->private);
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

static void
load_rtbm(RTbm *rtbm, ItemPointerData *itemptrs, int nitems)
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
			rtbm_add_tuples(rtbm,
							   curblkno, offs, noffs);
			curblkno = blkno;
			noffs = 0;
		}

		curblkno = blkno;
		offs[noffs++] = ItemPointerGetOffsetNumber(tid);
	}

	rtbm_add_tuples(rtbm, curblkno, offs, noffs);
}

/* ---------- radix ---------- */
static void
radix_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "radix bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = palloc(sizeof(bfm_tree));
	bfm_init(lvtt->private);
	MemoryContextSwitchTo(old_ctx);
}
static void
radix_fini(LVTestType *lvtt)
{
#if 0
	if (lvtt->private)
		bfm_free((RTbm *) lvtt->private);
#endif
}

/* log(sizeof(bfm_value_type) * BITS_PER_BYTE, 2) = log(64, 2) = 6 */
#define ENCODE_BITS 6

static uint64
radix_to_key_off(ItemPointer tid, uint32 *off)
{
	uint64 upper;
	uint32 shift = pg_ceil_log2_32(MaxHeapTuplesPerPage);
	int64 tid_i;

	Assert(ItemPointerGetOffsetNumber(tid) < MaxHeapTuplesPerPage);

	tid_i = ItemPointerGetOffsetNumber(tid);
	tid_i |= ItemPointerGetBlockNumber(tid) << shift;

	*off = tid_i & ((1 << ENCODE_BITS)-1);
	upper = tid_i >> ENCODE_BITS;
	Assert(*off < (sizeof(bfm_value_type) * BITS_PER_BYTE));

	Assert(*off < 64);

	return tid_i;
}

static void
radix_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			   BlockNumber maxblk, OffsetNumber maxoff)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(lvtt->mcxt);

	radix_load(lvtt->private,
			   DeadTuples_orig->itemptrs,
			   DeadTuples_orig->dtinfo.nitems);

	MemoryContextSwitchTo(oldcontext);
}


static bool
radix_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	uint64 key;
	uint32 off;
	bfm_value_type val;

	key = radix_to_key_off(itemptr, &off);

	if (!bfm_lookup((bfm_tree *) lvtt->private, key, &val))
		return false;

	return val & ((bfm_value_type)1 << off);
}

static uint64
radix_mem_usage(LVTestType *lvtt)
{
	bfm_tree *root = lvtt->private;
	size_t mem = MemoryContextMemAllocated(lvtt->mcxt, true);
	StringInfo s;

	s = bfm_stats(root);

	ereport(NOTICE,
			errmsg("radix tree of %.2f MB, %s",
				   (double) mem / (1024 * 1024),
				   s->data),
			errhidestmt(true),
			errhidecontext(true));

	pfree(s->data);
	pfree(s);

	return mem;
}

static void
radix_load(void *tbm, ItemPointerData *itemptrs, int nitems)
{
	bfm_tree *root = (bfm_tree *) tbm;
	uint64 last_key = PG_UINT64_MAX;
	uint64 val = 0;

	for (int i = 0; i < nitems; i++)
	{
		ItemPointer tid = &(itemptrs[i]);
		uint64 key;
		uint32 off;

		key = radix_to_key_off(tid, &off);

		if (last_key != PG_UINT64_MAX &&
			last_key != key)
		{
			bfm_set(root, last_key, val);
			val = 0;
		}

		last_key = key;
		val |= (uint64)1 << off;
	}

	if (last_key != PG_UINT64_MAX)
	{
		bfm_set(root, last_key, val);
	}
}

/* ------------ svtm ----------- */
static void
svtm_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "svtm bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = svtm_create();
	MemoryContextSwitchTo(old_ctx);
}

static void
svtm_fini(LVTestType *lvtt)
{
	if (lvtt->private != NULL)
		svtm_free(lvtt->private);
}

static void
svtm_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
			   BlockNumber maxblk, OffsetNumber maxoff)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(lvtt->mcxt);

	svtm_load(lvtt->private,
			   DeadTuples_orig->itemptrs,
			   DeadTuples_orig->dtinfo.nitems);

	MemoryContextSwitchTo(oldcontext);
}

static bool
svtm_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	return svtm_lookup(lvtt->private, itemptr);
}

static uint64
svtm_mem_usage(LVTestType *lvtt)
{
	svtm_stats((SVTm *) lvtt->private);
	return MemoryContextMemAllocated(lvtt->mcxt, true);
}

static void
svtm_load(SVTm *svtm, ItemPointerData *itemptrs, int nitems)
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
			svtm_add_page(svtm, curblkno, offs, noffs);
			curblkno = blkno;
			noffs = 0;
		}

		curblkno = blkno;
		offs[noffs++] = ItemPointerGetOffsetNumber(tid);
	}

	svtm_add_page(svtm, curblkno, offs, noffs);
	svtm_finalize_addition(svtm);
}

/* ---------- radix_tree ---------- */
static void
radix_tree_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "radix_tree bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);
	lvtt->private = radix_tree_create(lvtt->mcxt);
	MemoryContextSwitchTo(old_ctx);
}
static void
radix_tree_fini(LVTestType *lvtt)
{
#if 0
	if (lvtt->private)
		radix_tree_destroy((radix_tree *) llvt->private);
#endif
}

static void
radix_tree_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
				  BlockNumber maxblk, OffsetNumber maxoff)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(lvtt->mcxt);

	radix_tree_load(lvtt->private,
					DeadTuples_orig->itemptrs,
					DeadTuples_orig->dtinfo.nitems);

	MemoryContextSwitchTo(oldcontext);
}


static bool
radix_tree_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	uint64 key;
	uint32 off;
	bool found = false;

	key = radix_to_key_off(itemptr, &off);

	radix_tree_search((radix_tree *) lvtt->private, key, &found);

	return found;
}

static uint64
radix_tree_mem_usage(LVTestType *lvtt)
{
	radix_tree *tree = (radix_tree *) lvtt->private;
	uint64 mem = radix_tree_memory_usage(tree);

	radix_tree_stats(tree);

	ereport(NOTICE,
			errmsg("radix tree of %.2f MB",
				   (double) mem / (1024 * 1024)),
			errhidestmt(true),
			errhidecontext(true));

	return mem;
}

static void
radix_tree_load(void *tbm, ItemPointerData *itemptrs, int nitems)
{
	radix_tree *root = (radix_tree *) tbm;
	bool found;

	for (int i = 0; i < nitems; i++)
	{
		ItemPointer tid = &(itemptrs[i]);
		uint64 key;
		uint32 off;

		key = radix_to_key_off(tid, &off);

		radix_tree_insert(root, key, Int32GetDatum(100), &found);
	}
}

/* ---------- hash ---------- */
static void
hash_init(LVTestType *lvtt, uint64 nitems)
{
	MemoryContext old_ctx;
	static HTAB *h;
	HASHCTL ctl;

	lvtt->mcxt = AllocSetContextCreate(TopMemoryContext,
									   "hash bench",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

	ctl.keysize = sizeof(ItemPointerData);
	ctl.entrysize = sizeof(ItemPointerData);
	ctl.hcxt = lvtt->mcxt;
	h = hash_create("hash bench", nitems, &ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	lvtt->private = (void *) h;
	MemoryContextSwitchTo(old_ctx);
}
static void
hash_fini(LVTestType *lvtt)
{
#if 0
	if (lvtt->private)
		radix_tree_destroy((radix_tree *) llvt->private);
#endif
}

static void
hash_attach(LVTestType *lvtt, uint64 nitems, BlockNumber minblk,
				  BlockNumber maxblk, OffsetNumber maxoff)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(lvtt->mcxt);
	HTAB *h = (HTAB *) lvtt->private;
	bool found;

	for (int i = 0; i < nitems; i++)
	{
		hash_search(h,
					(void *) &(DeadTuples_orig->itemptrs[i]),
					HASH_ENTER, &found);
	}

	MemoryContextSwitchTo(oldcontext);
}


static bool
hash_reaped(LVTestType *lvtt, ItemPointer itemptr)
{
	bool found = false;
	HTAB *h = (HTAB *) lvtt->private;

	hash_search(h, (void *) itemptr, HASH_FIND, &found);

	return found;
}

static uint64
hash_mem_usage(LVTestType *lvtt)
{
	return MemoryContextMemAllocated(lvtt->mcxt, true);
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

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

	lvtt->attach_fn(lvtt, nitems, minblk, maxblk, maxoff);

	MemoryContextSwitchTo(old_ctx);
}

static void
_bench(LVTestType *lvtt)
{
	uint64 matched = 0;
	MemoryContext old_ctx;

#ifdef DEBUG_DUMP_MATCHED
	FILE *f = fopen(lvtt->name, "w");
#endif

	if (!lvtt->private)
		elog(ERROR, "%s dead tuples are not preapred", lvtt->name);

	old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

	for (int i = 0; i < IndexTids_cache->dtinfo.nitems; i++)
	{
		CHECK_FOR_INTERRUPTS();
		if (lvtt->reaped_fn(lvtt, &(IndexTids_cache->itemptrs[i])))
		{
#ifdef DEBUG_DUMP_MATCHED
			char buf[128] = {0};
			sprintf(buf, "(%5u, %5u)\n",
					 ItemPointerGetBlockNumber(&(IndexTids_cache->itemptrs[i])),
					 ItemPointerGetOffsetNumber(&(IndexTids_cache->itemptrs[i])));
			fwrite(buf, strlen(buf), 1, f);
#endif
			matched++;
		}
	}

	MemoryContextSwitchTo(old_ctx);

#ifdef DEBUG_DUMP_MATCHED
	fclose(f);
#endif

	if (matched != lvtt->dtinfo.nitems)
		elog(WARNING, "the number of dead tuples found doesn't match the actual dead tuples: got %lu expected %lu",
			 matched, lvtt->dtinfo.nitems);

	elog(NOTICE, "\"%s\": dead tuples %lu, index tuples %lu, matched %lu, mem %zu",
//	elog(NOTICE, "\"%s\": dead tuples %lu, index tuples %lu, matched %lu, mem %.2f",
		 lvtt->name,
		 lvtt->dtinfo.nitems,
		 IndexTids_cache->dtinfo.nitems,
		 matched,
		 lvtt->mem_usage_fn(lvtt));
//		 (double) lvtt->mem_usage_fn(lvtt) / (1024 * 1024));
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
prepare(PG_FUNCTION_ARGS)
{
	BlockNumber maxblk = PG_GETARG_INT64(0);
	uint64 ndeadtuples_in_page = PG_GETARG_INT32(1);
	uint64 interval_in_page = PG_GETARG_INT32(2);
	uint64 page_consecutives = PG_GETARG_INT32(3);
	uint64 page_interval = PG_GETARG_INT32(4);
	bool shuffle = PG_GETARG_BOOL(5);

	OffsetNumber maxoff = ndeadtuples_in_page * interval_in_page;
	uint64 ndts = 0;
	uint64 nidx = 0;
	uint64 ndts_tmp = 0;
	uint64 nidx_tmp = 0;

	if (page_consecutives > page_interval)
		elog(ERROR, "cannot prepare %lu consecutive dirty pages at %lu pages interval",
			 page_consecutives, page_interval);

	ndts = ((uint64) ceil((double)maxblk / page_interval) * page_consecutives) * ndeadtuples_in_page;
	DeadTuples_orig = MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(DeadTuplesArray));
	DeadTuples_orig->itemptrs =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * ndts);

	nidx = ((uint64) maxblk) * ((uint64) maxoff);
	IndexTids_cache = MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(DeadTuplesArray));
	IndexTids_cache->itemptrs =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * nidx);

	elog(WARNING, "dead tuples: page: total %lu tuples, %lu tuples with interval %lu in page (maxoff %u, shuffle %d), blk: maxblk %u consecutive %lu interval %lu, setting: ndts %lu nidx %lu",
		 ndts,
		 ndeadtuples_in_page,
		 interval_in_page,
		 maxoff,
		 shuffle,
		 maxblk,
		 page_consecutives,
		 page_interval,
		 ndts, nidx);

	for (BlockNumber blkno = 0; blkno < maxblk; blkno++)
	{
		uint64 ndt_this_page = 0;

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemPointerData tid;

			ItemPointerSetBlockNumber(&tid, blkno);
			ItemPointerSetOffsetNumber(&tid, off);

			if ((blkno % page_interval) < page_consecutives &&
				off % interval_in_page == 0 &&
				ndt_this_page <= ndeadtuples_in_page)
			{
				ndt_this_page++;
				DeadTuples_orig->itemptrs[ndts_tmp++] = tid;
			}
			IndexTids_cache->itemptrs[nidx_tmp++] = tid;
		}
	}

	/* sanity checks */
	if (ndts_tmp > ndts)
		elog(ERROR, "ndts_tmp %lu ndts %lu", ndts_tmp, ndts);
	if (nidx_tmp > nidx)
		elog(ERROR, "nidx_tmp %lu nidx %lu", nidx_tmp, nidx);

	/* Shuffle index tuples */
	if (shuffle)
		shuffle_itemptrs(nidx, IndexTids_cache->itemptrs);

	DeadTuples_orig->dtinfo.nitems = Min(ndts, ndts_tmp);
	IndexTids_cache->dtinfo.nitems = Min(nidx, nidx_tmp);

	PG_RETURN_VOID();
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

			old_ctx = MemoryContextSwitchTo(lvtt->mcxt);

			attach(lvtt,
				   DeadTuples_orig->dtinfo.nitems,
				   DeadTuples_orig->dtinfo.minblk,
				   DeadTuples_orig->dtinfo.maxblk,
				   DeadTuples_orig->dtinfo.maxoff);

			MemoryContextSwitchTo(old_ctx);

			break;
		}
	}

	elog(WARNING, "attached dead tuples to %s", mode);

	PG_RETURN_VOID();
}

Datum
bench(PG_FUNCTION_ARGS)
{
	char *mode = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!IndexTids_cache || !IndexTids_cache->itemptrs)
		elog(ERROR, "index tuples are not preapred");

	for (int i = 0; i < TEST_SUBJECT_TYPES; i++)
	{
		LVTestType *lvtt = &(LVTestSubjects[i]);
		if (strcmp(mode, lvtt->name) == 0)
		{
			_bench(lvtt);
			break;
		}
	}

	PG_RETURN_NULL();
}

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

Datum
rtbm_test(PG_FUNCTION_ARGS)
{
	RTbm *rtbm = rtbm_create();
	IntegerSet *intset = intset_create();
	int matched_intset = 0, matched_rtbm = 0;
	const int nitems_dead = 1000;
	const int nitems_index = 10000;
	ItemPointerData *dead_tuples =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * nitems_dead);
	ItemPointerData *index_tuples =
		(ItemPointer) MemoryContextAllocHuge(TopMemoryContext,
											 sizeof(ItemPointerData) * nitems_index);

	generate_random_itemptrs(nitems_index, 0, 10000, 100, index_tuples);
	generate_random_itemptrs(nitems_dead, 0, 1000, 100, dead_tuples);

	for (int i = 0; i < nitems_dead; i++)
		intset_add_member(intset, itemptr_encode(&dead_tuples[i]));
	load_rtbm(rtbm, dead_tuples, nitems_dead);

	for (int i = 0; i < nitems_index; i++)
	{
		bool ret1, ret2;

		CHECK_FOR_INTERRUPTS();

		ret1 = intset_is_member(intset, itemptr_encode(&(index_tuples[i])));
		ret2 = rtbm_lookup(rtbm, &(index_tuples[i]));

		if (i % 10000000 == 0)
			elog(NOTICE, "%d done", i);

		if (ret1 != ret2)
		{
			rtbm_dump_blk(rtbm, i);
			elog(ERROR, "failed (%d, %d) : intset %d rtbm %d",
				 ItemPointerGetBlockNumber(&(index_tuples[i])),
				 ItemPointerGetOffsetNumber(&(index_tuples[i])),
				 ret1, ret2);
		}

		if (ret1)
			matched_intset++;
		if (ret2)
			matched_rtbm++;
	}

	rtbm_dump(rtbm);
	elog(NOTICE, "matched intset %d rtbm %d",
		 matched_intset,
		 matched_rtbm);
	PG_RETURN_NULL();

}

Datum
radix_run_tests(PG_FUNCTION_ARGS)
{
	LVTestType *tree1;
	LVTestType *tree2;
	uint64 nmatched1 = 0, nmatched2 = 0;

	DirectFunctionCall6(prepare,
						Int64GetDatum(1000000),
						Int32GetDatum(10),
						Int32GetDatum(1),
						Int32GetDatum(1),
						Int32GetDatum(20),
						BoolGetDatum(true));

	DirectFunctionCall1(attach_dead_tuples,
						CStringGetDatum(cstring_to_text("intset")));
	DirectFunctionCall1(attach_dead_tuples,
						CStringGetDatum(cstring_to_text("radix_tree")));

	tree1 = &(LVTestSubjects[2]);
	tree2 = &(LVTestSubjects[7]);

	elog(NOTICE, "tree1 name %s", tree1->name);
	elog(NOTICE, "tree2 name %s", tree2->name);

	for (int i = 0; i < IndexTids_cache->dtinfo.nitems; i++)
	{
		bool match1, match2;

		CHECK_FOR_INTERRUPTS();

		match1 = tree1->reaped_fn(tree1, &(IndexTids_cache->itemptrs[i]));
		match2 = tree2->reaped_fn(tree2, &(IndexTids_cache->itemptrs[i]));

		if (match1)
			nmatched1++;
		if (match2)
			nmatched2++;

		if (match1 != match2)
		{
			uint64 key;
			uint32 dummy;

			key = radix_to_key_off(&(IndexTids_cache->itemptrs[i]), &dummy);

			elog(NOTICE, "ERR: tid = (%u,%u) key = %lX intset = %s radix = %s",
				 ItemPointerGetBlockNumber(&(IndexTids_cache->itemptrs[i])),
				 ItemPointerGetOffsetNumber(&(IndexTids_cache->itemptrs[i])),
				 key,
				 match1 ? "OK" : "NG",
				 match2 ? "OK" : "NG");
		}
	}

	elog(NOTICE, "RES: bfm matched = %lu radix matched = %lu",
		 nmatched1, nmatched2);

	ItemPointerData item;
	uint64 ikey;
	uint32 dummy;

	ItemPointerSetBlockNumber(&item, 60);
	ItemPointerSetOffsetNumber(&item, 6);
	ikey = radix_to_key_off(&item, &dummy);

	tree2->reaped_fn(tree2, &item);

	tree2->mem_usage_fn(tree2);

	PG_RETURN_VOID();
}
