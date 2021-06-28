#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "rtbm.h"

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

typedef struct RTbm
{
	struct dttable_hash *dttable;
	int		dttable_size;

	int		npages;

	int		curr_offset;
	char	*bitmap;
	int		bitmap_size;
} RTbm;

#define RTBM_BITMAP_INITIAL_SIZE	(64 * 1024) /* 64kB */

#define WORDNUM(x) ((x) / 8)
#define BITNUM(x) ((x) % 8)

static void enlarge_space(RTbm *rtbm)
{
	int newsize = rtbm->bitmap_size * 2;
	char *new = palloc0(newsize);

	elog(NOTICE, "enlarge %d to %d", rtbm->bitmap_size, newsize);

	memcpy(new, rtbm->bitmap, rtbm->bitmap_size);
	pfree(rtbm->bitmap);
	rtbm->bitmap = new;
	rtbm->bitmap_size = newsize;
}

RTbm *
rtbm_create(void)
{
	RTbm *rtbm = palloc0(sizeof(RTbm));

	rtbm->dttable = dttable_create(CurrentMemoryContext, 128, (void *) rtbm);

	rtbm->bitmap = palloc0(RTBM_BITMAP_INITIAL_SIZE);
	rtbm->bitmap_size = RTBM_BITMAP_INITIAL_SIZE;

	return rtbm;
}

void
rtbm_free(RTbm *rtbm)
{
	pfree(rtbm->bitmap);
	pfree(rtbm);
}

void
rtbm_add_tuples(RTbm *rtbm, const BlockNumber blkno,
				   const OffsetNumber *offnums, int nitems)
{
	DtEntry *entry;
	bool	found;
	char	oldstatus;
	int		 wordnum, bitnum;
	int		array_size, bitmap_size;
	OffsetNumber maxoff = FirstOffsetNumber;

	entry = dttable_insert(rtbm->dttable, blkno, &found);
	Assert(found);

	/* initialize entry */
	oldstatus = entry->status;
	MemSet(entry, 0, sizeof(DtEntry));
	entry->status = oldstatus;
	entry->blkno = blkno;
	entry->offset = rtbm->curr_offset;

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
		OffsetNumber *off_p = (OffsetNumber *) &(rtbm->bitmap[entry->offset]);

		/* Go with array container */
		entry->flags |= DTENTRY_FLAGS_TYPE_ARRAY;

		if (rtbm->curr_offset >= rtbm->bitmap_size)
			enlarge_space(rtbm);

		for (int i = 0; i < nitems; i++)
			*(off_p + i) = offnums[i];

		entry->flags |= (((uint16) nitems) & DTENTRY_FLAGS_LEN_MASK);
		rtbm->curr_offset += ((sizeof(OffsetNumber) * nitems) + 1);
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

			if (wordnum + rtbm->curr_offset >= rtbm->bitmap_size)
				enlarge_space(rtbm);

			rtbm->bitmap[entry->offset + wordnum] |= (1 << bitnum);
		}

		entry->flags |= (((uint16) (wordnum + 1) * 8) & DTENTRY_FLAGS_LEN_MASK);
		rtbm->curr_offset += (wordnum + 1);
	}

	rtbm->npages++;
}

bool
rtbm_lookup(RTbm *rtbm, ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	DtEntry *entry;
	int wordnum, bitnum;
	bool ret = false;
	uint16 len;

	entry = dttable_lookup(rtbm->dttable, blk);

	if (!entry)
		return false;

	len = (uint16) (entry->flags & DTENTRY_FLAGS_LEN_MASK);

	if (DTENTRY_IS_ARRAY(entry))
	{
		OffsetNumber *off_p = (OffsetNumber *) &(rtbm->bitmap[entry->offset]);
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

			ret = ((rtbm->bitmap[entry->offset + wordnum] & (1 << bitnum)) != 0);
		}
	}
	else
		elog(ERROR, "invalid container type");

	return ret;
}

static inline void *
dttable_allocate(dttable_hash *dttable, Size size)
{
	RTbm *rtbm = (RTbm *) dttable->private_data;

	rtbm->dttable_size = size;
	return MemoryContextAllocExtended(dttable->ctx, size,
									  MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
}

static inline void
dttable_free(dttable_hash *dttable, void *pointer)
{
	pfree(pointer);
}

static int
rtbm_comparator(const void *left, const void *right)
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
rtbm_stats(RTbm *rtbm)
{
	elog(NOTICE, "dtatble_size %d bitmap_size %d npages %d, offset %d",
		 rtbm->dttable_size,
		 rtbm->bitmap_size,
		 rtbm->npages,
		 rtbm->curr_offset);
	elog(NOTICE, "sizeof(DtEntry) %lu", sizeof(DtEntry));
}

static void
dump_entry(RTbm *rtbm, DtEntry *entry)
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
		OffsetNumber *off_p = (OffsetNumber *) &(rtbm->bitmap[entry->offset]);

		for (int i = 0; i < len; i++)
			appendStringInfo(&str, "%d ", *(off_p + i));
	}
	else
	{
		char *bitmap = &(rtbm->bitmap[entry->offset]);

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
rtbm_dump(RTbm *rtbm)
{
	dttable_iterator iter;
	DtEntry *entry;
	DtEntry **entries;
	int num = 0;

	entries = (DtEntry **) palloc(rtbm->npages * sizeof(DtEntry *));

	dttable_start_iterate(rtbm->dttable, &iter);
	while ((entry = dttable_iterate(rtbm->dttable, &iter)) != NULL)
		entries[num++] = entry;

	qsort(entries, rtbm->npages, sizeof(DtEntry *), rtbm_comparator);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 rtbm->bitmap_size, rtbm->npages);
	for (int i = 0; i < rtbm->npages; i++)
	{
		entry = entries[i];

		dump_entry(rtbm, entry);
	}
}

void
rtbm_dump_blk(RTbm *rtbm, BlockNumber blkno)
{
	DtEntry *entry;
	StringInfoData str;

	initStringInfo(&str);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 rtbm->bitmap_size, rtbm->npages);
	entry = dttable_lookup(rtbm->dttable, blkno);

	if (!entry)
	{
		elog(NOTICE, "NOT FOUND blkno %u", blkno);
		return;
	}

	dump_entry(rtbm, entry);
}
