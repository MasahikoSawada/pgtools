#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "lvdeadtuple.h"

typedef struct DtEntry
{
	BlockNumber blkno;
	char		status;
	uint16		len;
	uint32		offset;
} DtEntry;

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

typedef struct DeadTupleStore
{
	struct dttable_hash *dttable;
	int		dttable_size;

	int		npages;

	int		curr_offset;
	char	*bitmap;
	int		bitmap_size;
} DeadTupleStore;

#define DTSTORE_BITMAP_CHUNK_SIZE	(64 * 1024) /* 64kB */
//#define DTSTORE_BITMAP_CHUNK_SIZE	(1024) /* 1kB */

#define WORDNUM(x) ((x) / 8)
#define BITNUM(x) ((x) % 8)

DeadTupleStore *
dtstore_create(void)
{
	DeadTupleStore *dtstore = palloc0(sizeof(DeadTupleStore));

	dtstore->dttable = dttable_create(CurrentMemoryContext, 128, (void *) dtstore);

	dtstore->bitmap = palloc0(DTSTORE_BITMAP_CHUNK_SIZE);
	dtstore->bitmap_size = DTSTORE_BITMAP_CHUNK_SIZE;

	return dtstore;
}

void
dtstore_free(DeadTupleStore *dtstore)
{
	pfree(dtstore->bitmap);
	pfree(dtstore);
}

void
dtstore_add_tuples(DeadTupleStore *dtstore, const BlockNumber blkno,
				   const OffsetNumber *offnums, int nitems)
{
	DtEntry *entry;
	bool	found;
	char	oldstatus;
	int wordnum, bitnum;

	entry = dttable_insert(dtstore->dttable, blkno, &found);
	Assert(!found);

	/* initialize entry */
	oldstatus = entry->status;
	MemSet(entry, 0, sizeof(DtEntry));
	entry->status = oldstatus;
	entry->blkno = blkno;

	entry->offset = dtstore->curr_offset;
	for (int i = 0; i < nitems; i++)
	{
		OffsetNumber off = offnums[i];

		wordnum = WORDNUM(off - 1);
		bitnum = BITNUM(off - 1);

		/* enlarge bitmap space */
		if (wordnum + dtstore->curr_offset >= dtstore->bitmap_size)
		{
			int newsize = dtstore->bitmap_size * 2;
			char *new = palloc0(newsize);

			elog(NOTICE, "enlarge %d to %d", dtstore->bitmap_size, newsize);

			memcpy(new, dtstore->bitmap, dtstore->bitmap_size);
			pfree(dtstore->bitmap);
			dtstore->bitmap = new;
			dtstore->bitmap_size = newsize;
		}

		dtstore->bitmap[entry->offset + wordnum] |= (1 << bitnum);
	}

	entry->len = (wordnum + 1) * 8;

	dtstore->curr_offset += (wordnum + 1);
	dtstore->npages++;
}

bool
dtstore_lookup(DeadTupleStore *dtstore, ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	DtEntry *entry;
	int wordnum, bitnum;

	entry = dttable_lookup(dtstore->dttable, blk);

	if (!entry)
		return false;

	if (entry->len <= off - 1)
		return false;

	wordnum = WORDNUM(off - 1);
	bitnum = BITNUM(off - 1);

	/*
	fprintf(stderr, "LOOKUP (%d,%d) WORD %d BIT %d\n",
			blk, off,
			wordnum, bitnum);
	for (int i = 0; i < entry->len / 8; i++)
		fprintf(stderr, "%02X",
				dtstore->bitmap[entry->offset + i] & 0x000000FF);
//		fprintf(stderr, "%s",
//				dtstore->bitmap[entry->offset + i % 8] & ((char) 1 << i) ? "1" : "0");
	fprintf(stderr, "\n");
	fprintf(stderr, "%02X %02X -> %d\n",
			dtstore->bitmap[entry->offset + wordnum]& 0x000000FF,
			(1 << bitnum) & 0x000000FF,
			dtstore->bitmap[entry->offset + wordnum] & (1 << bitnum));
	fflush(stderr);
	*/

	return ((dtstore->bitmap[entry->offset + wordnum] & (1 << bitnum)) != 0);
}

static inline void *
dttable_allocate(dttable_hash *dttable, Size size)
{
	DeadTupleStore *dtstore = (DeadTupleStore *) dttable->private_data;

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
dtstore_stats(DeadTupleStore *dtstore)
{
	elog(NOTICE, "dtatble_size %d bitmap_size %d npages %d, offset %d",
		 dtstore->dttable_size,
		 dtstore->bitmap_size,
		 dtstore->npages,
		 dtstore->curr_offset);
	elog(NOTICE, "sizeof(DtEntry) %lu", sizeof(DtEntry));
}

void
dtstore_dump(DeadTupleStore *dtstore)
{
	dttable_iterator iter;
	DtEntry *entry;
	DtEntry **entries;
	int num = 0;
	StringInfoData str;

	entries = (DtEntry **) palloc(dtstore->npages * sizeof(DtEntry *));

	dttable_start_iterate(dtstore->dttable, &iter);
	while ((entry = dttable_iterate(dtstore->dttable, &iter)) != NULL)
		entries[num++] = entry;

	qsort(entries, dtstore->npages, sizeof(DtEntry *), dtstore_comparator);

	initStringInfo(&str);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 dtstore->bitmap_size, dtstore->npages);
	for (int i = 0; i < dtstore->npages; i++)
	{
		entry = entries[i];
		char *bitmap = &(dtstore->bitmap[entry->offset]);

		appendStringInfo(&str, "[%5d] : ", entry->blkno);
		for (int off = 0; off < entry->len; off++)
		{
			int wordnum = WORDNUM(off - 1), bitnum = BITNUM(off - 1);

			appendStringInfo(&str, "%s",
							 ((bitmap[wordnum] & ((char) 1 << bitnum)) != 0) ? "1" : "0");
		}
		elog(NOTICE, "%s (offset %d len %d)",
			 str.data,
			 entry->offset, entry->len);
		resetStringInfo(&str);
	}
}

void
dtstore_dump_blk(DeadTupleStore *dtstore, BlockNumber blkno)
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

	char *bitmap = &(dtstore->bitmap[entry->offset]);

	appendStringInfo(&str, "[%5d] : ", entry->blkno);
	for (int off = 1; off < entry->len; off++)
	{
		int wordnum = WORDNUM(off - 1), bitnum = BITNUM(off - 1);

		appendStringInfo(&str, "%s",
						 ((bitmap[wordnum] & ((char) 1 << bitnum)) != 0) ? "1" : "0");
		if (off % 10 == 0)
			appendStringInfo(&str, "%s", " ");
	}
	elog(NOTICE, "%s (offset %d len %d)",
		 str.data,
		 entry->offset, entry->len);
	appendStringInfo(&str, "[%5d] : ", entry->blkno);

	for (int off = 1; off < entry->len; off++)
	{
		int wordnum = WORDNUM(off - 1), bitnum = BITNUM(off - 1);

		appendStringInfo(&str, "%s",
						 ((bitmap[wordnum] & ((char) 1 << bitnum)) != 0) ? "1" : "0");
		if (off % 8 == 0)
			appendStringInfo(&str, "%s", " ");
	}
	elog(NOTICE, "%s (offset %d len %d)",
		 str.data,
		 entry->offset, entry->len);
}
