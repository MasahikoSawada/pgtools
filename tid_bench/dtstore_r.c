#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "dtstore_r.h"

#define DTENTRY_FLAGS_TYPE_ARRAY	0x1000
#define DTENTRY_FLAGS_TYPE_BITMAP	0x2000
#define DTENTRY_FLAGS_TYPE_RUN		0x4000
#define DTENTRY_FLAGS_LEN_MASK		0x0FFF

typedef struct DtEntry
{
	BlockNumber blkno;
	char		status;
	uint16		flags;
	uint32		offset;
} DtEntry;

#define DTENTRY_IS_ARRAY(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAGS_TYPE_ARRAY) != 0)
#define DTENTRY_IS_BITMAP(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAGS_TYPE_BITMAP) != 0)
#define DTENTRY_IS_RUN(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAGS_TYPE_RUN) != 0)

#define SH_USE_NONDEFAULT_ALLOCATOR
#define SH_PREFIX dttable
#define SH_ELEMENT_TYPE DtEntry
#define SH_KEY_TYPE BlockNumber
#define SH_KEY blkno
#define SH_HASH_KEY(tb, key) murmurhash32(key)
#define SH_EQUAL(tb, a, b) a == b
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

typedef struct DeadTupleStoreR
{
	struct dttable_hash *dttable;
	int		dttable_size;

	int		npages;

	int		curr_offset;
	char	*bitmap;
	int		bitmap_size;
} DeadTupleStoreR;

#define DTSTORE_BITMAP_CHUNK_SIZE	(64 * 1024) /* 64kB */
//#define DTSTORE_BITMAP_CHUNK_SIZE	(1024) /* 1kB */

#define WORDNUM(x) ((x) / 8)
#define BITNUM(x) ((x) % 8)

static void enlarge_space(DeadTupleStoreR *dtstore)
{
	int newsize = dtstore->bitmap_size * 2;
	char *new = palloc0(newsize);

	elog(NOTICE, "enlarge %d to %d", dtstore->bitmap_size, newsize);

	memcpy(new, dtstore->bitmap, dtstore->bitmap_size);
	pfree(dtstore->bitmap);
	dtstore->bitmap = new;
	dtstore->bitmap_size = newsize;
}

DeadTupleStoreR *
dtstore_r_create(void)
{
	DeadTupleStoreR *dtstore = palloc0(sizeof(DeadTupleStoreR));

	dtstore->dttable = dttable_create(CurrentMemoryContext, 128, (void *) dtstore);

	dtstore->bitmap = palloc0(DTSTORE_BITMAP_CHUNK_SIZE);
	dtstore->bitmap_size = DTSTORE_BITMAP_CHUNK_SIZE;

	return dtstore;
}

void
dtstore_r_free(DeadTupleStoreR *dtstore)
{
	pfree(dtstore->bitmap);
	pfree(dtstore);
}

void
dtstore_r_add_tuples(DeadTupleStoreR *dtstore, const BlockNumber blkno,
				   const OffsetNumber *offnums, int nitems)
{
	DtEntry *entry;
	bool	found;
	char	oldstatus;
	int		 wordnum, bitnum;
	int		array_size, bitmap_size;
	OffsetNumber maxoff = FirstOffsetNumber;

	entry = dttable_insert(dtstore->dttable, blkno, &found);
	Assert(found);

	/* initialize entry */
	oldstatus = entry->status;
	MemSet(entry, 0, sizeof(DtEntry));
	entry->status = oldstatus;
	entry->blkno = blkno;
	entry->offset = dtstore->curr_offset;

	/* get the highest offset number */
	for (int i = 0; i < nitems; i++)
	{
		if (offnums[i] > maxoff)
			maxoff = offnums[i];
	}

	/* calculate the space needed by each strategy in byte */
	array_size = nitems * sizeof(OffsetNumber);
	bitmap_size = (maxoff / 8) + 1;

	/* enlarge bitmap space */
	{
	}

	if (array_size <= bitmap_size)
	{
		OffsetNumber *off_p = (OffsetNumber *) &(dtstore->bitmap[entry->offset]);

		/* Go with array container */
		entry->flags |= DTENTRY_FLAGS_TYPE_ARRAY;

		if (dtstore->curr_offset >= dtstore->bitmap_size)
			enlarge_space(dtstore);

		for (int i = 0; i < nitems; i++)
			*(off_p + i) = offnums[i];

		entry->flags |= (((uint16) nitems) & DTENTRY_FLAGS_LEN_MASK);
		dtstore->curr_offset += ((sizeof(OffsetNumber) * nitems) + 1);
	}
	else
	{
		/* Go with bitmap container */
		entry->flags |= DTENTRY_FLAGS_TYPE_BITMAP;

		for (int i = 0; i < nitems; i++)
		{
			OffsetNumber off = offnums[i];

			wordnum = WORDNUM(off - 1);
			bitnum = BITNUM(off - 1);

			if (wordnum + dtstore->curr_offset >= dtstore->bitmap_size)
				enlarge_space(dtstore);

			dtstore->bitmap[entry->offset + wordnum] |= (1 << bitnum);
		}

		entry->flags |= (((uint16) (wordnum + 1) * 8) & DTENTRY_FLAGS_LEN_MASK);
		dtstore->curr_offset += (wordnum + 1);
	}

	dtstore->npages++;
}

bool
dtstore_r_lookup(DeadTupleStoreR *dtstore, ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	DtEntry *entry;
	int wordnum, bitnum;
	bool ret = false;
	uint16 len;

	entry = dttable_lookup(dtstore->dttable, blk);

	if (!entry)
		return false;

	len = (uint16) (entry->flags & DTENTRY_FLAGS_LEN_MASK);

	if (DTENTRY_IS_ARRAY(entry))
	{
		OffsetNumber *off_p = (OffsetNumber *) &(dtstore->bitmap[entry->offset]);
		ret = false;

		for (int i = 0; i < len; i++)
		{
			if (*(off_p + i) == off)
			{
				ret = true;
				break;
			}
		}
	}
	else if (DTENTRY_IS_BITMAP(entry))
	{
		if (len <= off - 1)
			ret = false;
		else
		{
			wordnum = WORDNUM(off - 1);
			bitnum = BITNUM(off - 1);

			ret = ((dtstore->bitmap[entry->offset + wordnum] & (1 << bitnum)) != 0);
		}
	}
	else
		elog(ERROR, "invalid container type");

	return ret;
}

static inline void *
dttable_allocate(dttable_hash *dttable, Size size)
{
	DeadTupleStoreR *dtstore = (DeadTupleStoreR *) dttable->private_data;

	dtstore->dttable_size = size;
	return MemoryContextAllocExtended(dttable->ctx, size,
									  MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
}

static inline void
dttable_free(dttable_hash *dttable, void *pointer)
{
	pfree(pointer);
}

static int
dtstore_comparator(const void *left, const void *right)
{
	BlockNumber l = (*((DtEntry *const *) left))->blkno;
	BlockNumber r = (*((DtEntry *const *) right))->blkno;

	if (l < r)
		return -1;
	else if (l > r)
		return 1;
	return 0;
}

void
dtstore_r_stats(DeadTupleStoreR *dtstore)
{
	elog(NOTICE, "dtatble_size %d bitmap_size %d npages %d, offset %d",
		 dtstore->dttable_size,
		 dtstore->bitmap_size,
		 dtstore->npages,
		 dtstore->curr_offset);
	elog(NOTICE, "sizeof(DtEntry) %lu", sizeof(DtEntry));
}

static void
dump_entry(DeadTupleStoreR *dtstore, DtEntry *entry)
{
	StringInfoData str;
	uint16 len;

	initStringInfo(&str);

	appendStringInfo(&str, "[%5d] (%-6s): ", entry->blkno,
					 DTENTRY_IS_ARRAY(entry) ? "ARRAY" :
					 DTENTRY_IS_BITMAP(entry) ? "BITMAP" :
					 DTENTRY_IS_RUN(entry) ? "RUN" : "UNKNOWN");

	len = (uint16) (entry->flags & DTENTRY_FLAGS_LEN_MASK);

	if (DTENTRY_IS_ARRAY(entry))
	{
		OffsetNumber *off_p = (OffsetNumber *) &(dtstore->bitmap[entry->offset]);

		for (int i = 0; i < len; i++)
			appendStringInfo(&str, "%d ", *(off_p + i));
	}
	else
	{
		char *bitmap = &(dtstore->bitmap[entry->offset]);

		for (int off = 0; off < len; off++)
		{
			int wordnum = WORDNUM(off - 1), bitnum = BITNUM(off - 1);

			appendStringInfo(&str, "%s",
							 ((bitmap[wordnum] & ((char) 1 << bitnum)) != 0) ? "1" : "0");
			if (off > 0 && off % 8 == 0)
				appendStringInfo(&str, "%s", " ");
		}
	}

	elog(NOTICE, "%s (offset %d len %d)",
		 str.data,
		 entry->offset, len);
}

void
dtstore_r_dump(DeadTupleStoreR *dtstore)
{
	dttable_iterator iter;
	DtEntry *entry;
	DtEntry **entries;
	int num = 0;

	entries = (DtEntry **) palloc(dtstore->npages * sizeof(DtEntry *));

	dttable_start_iterate(dtstore->dttable, &iter);
	while ((entry = dttable_iterate(dtstore->dttable, &iter)) != NULL)
		entries[num++] = entry;

	qsort(entries, dtstore->npages, sizeof(DtEntry *), dtstore_comparator);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 dtstore->bitmap_size, dtstore->npages);
	for (int i = 0; i < dtstore->npages; i++)
	{
		entry = entries[i];

		dump_entry(dtstore, entry);
	}
}

void
dtstore_r_dump_blk(DeadTupleStoreR *dtstore, BlockNumber blkno)
{
	DtEntry *entry;
	StringInfoData str;

	initStringInfo(&str);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 dtstore->bitmap_size, dtstore->npages);
	entry = dttable_lookup(dtstore->dttable, blkno);

	if (!entry)
	{
		elog(NOTICE, "NOT FOUND blkno %u", blkno);
		return;
	}

	dump_entry(dtstore, entry);
}
