#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "vtbm.h"

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

typedef struct VTbm
{
	struct dttable_hash *dttable;
	int		dttable_size;

	int		npages;

	int		curr_offset;
	char	*bitmap;
	int		bitmap_size;
} VTbm;

#define VTBM_BITMAP_INITIAL_SIZE	(64 * 1024) /* 64kB */

#define WORDNUM(x) ((x) / 8)
#define BITNUM(x) ((x) % 8)

VTbm *
vtbm_create(void)
{
	VTbm *vtbm = palloc0(sizeof(VTbm));

	vtbm->dttable = dttable_create(CurrentMemoryContext, 128, (void *) vtbm);

	vtbm->bitmap = palloc0(VTBM_BITMAP_INITIAL_SIZE);
	vtbm->bitmap_size = VTBM_BITMAP_INITIAL_SIZE;

	return vtbm;
}

void
vtbm_free(VTbm *vtbm)
{
	pfree(vtbm->bitmap);
	pfree(vtbm);
}

void
vtbm_add_tuples(VTbm *vtbm, const BlockNumber blkno,
				   const OffsetNumber *offnums, int nitems)
{
	DtEntry *entry;
	bool	found;
	char	oldstatus;
	int wordnum = 0;
	int bitnum;

	entry = dttable_insert(vtbm->dttable, blkno, &found);
	Assert(!found);

	/* initialize entry */
	oldstatus = entry->status;
	MemSet(entry, 0, sizeof(DtEntry));
	entry->status = oldstatus;
	entry->blkno = blkno;

	entry->offset = vtbm->curr_offset;
	for (int i = 0; i < nitems; i++)
	{
		OffsetNumber off = offnums[i];

		wordnum = WORDNUM(off - 1);
		bitnum = BITNUM(off - 1);

		/* enlarge bitmap space */
		if (wordnum + vtbm->curr_offset >= vtbm->bitmap_size)
		{
			int newsize = vtbm->bitmap_size * 2;
			char *new = palloc0(newsize);

			//elog(NOTICE, "enlarge %d to %d", vtbm->bitmap_size, newsize);

			memcpy(new, vtbm->bitmap, vtbm->bitmap_size);
			pfree(vtbm->bitmap);
			vtbm->bitmap = new;
			vtbm->bitmap_size = newsize;
		}

		vtbm->bitmap[entry->offset + wordnum] |= (1 << bitnum);
	}

	entry->len = (wordnum + 1) * 8;

	vtbm->curr_offset += (wordnum + 1);
	vtbm->npages++;
}

bool
vtbm_lookup(VTbm *vtbm, ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	DtEntry *entry;
	int wordnum, bitnum;

	entry = dttable_lookup(vtbm->dttable, blk);

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
				vtbm->bitmap[entry->offset + i] & 0x000000FF);
//		fprintf(stderr, "%s",
//				vtbm->bitmap[entry->offset + i % 8] & ((char) 1 << i) ? "1" : "0");
	fprintf(stderr, "\n");
	fprintf(stderr, "%02X %02X -> %d\n",
			vtbm->bitmap[entry->offset + wordnum]& 0x000000FF,
			(1 << bitnum) & 0x000000FF,
			vtbm->bitmap[entry->offset + wordnum] & (1 << bitnum));
	fflush(stderr);
	*/

	return ((vtbm->bitmap[entry->offset + wordnum] & (1 << bitnum)) != 0);
}

static inline void *
dttable_allocate(dttable_hash *dttable, Size size)
{
	VTbm *vtbm = (VTbm *) dttable->private_data;

	vtbm->dttable_size = size;
	return MemoryContextAllocExtended(dttable->ctx, size,
									  MCXT_ALLOC_HUGE | MCXT_ALLOC_ZERO);
}

static inline void
dttable_free(dttable_hash *dttable, void *pointer)
{
	pfree(pointer);
}

static int
vtbm_comparator(const void *left, const void *right)
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
vtbm_stats(VTbm *vtbm)
{
	elog(NOTICE, "dtatble_size %d bitmap_size %d npages %d, offset %d",
		 vtbm->dttable_size,
		 vtbm->bitmap_size,
		 vtbm->npages,
		 vtbm->curr_offset);
	elog(NOTICE, "sizeof(DtEntry) %lu", sizeof(DtEntry));
}

void
vtbm_dump(VTbm *vtbm)
{
	dttable_iterator iter;
	DtEntry *entry;
	DtEntry **entries;
	int num = 0;
	StringInfoData str;

	entries = (DtEntry **) palloc(vtbm->npages * sizeof(DtEntry *));

	dttable_start_iterate(vtbm->dttable, &iter);
	while ((entry = dttable_iterate(vtbm->dttable, &iter)) != NULL)
		entries[num++] = entry;

	qsort(entries, vtbm->npages, sizeof(DtEntry *), vtbm_comparator);

	initStringInfo(&str);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 vtbm->bitmap_size, vtbm->npages);
	for (int i = 0; i < vtbm->npages; i++)
	{
		char *bitmap;

		entry = entries[i];
		bitmap = &(vtbm->bitmap[entry->offset]);

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
vtbm_dump_blk(VTbm *vtbm, BlockNumber blkno)
{
	DtEntry *entry;
	StringInfoData str;
	char *bitmap;

	initStringInfo(&str);

	elog(NOTICE, "DEADTUPLESTORE (bitmap size %d, npages %d) ----------------------------",
		 vtbm->bitmap_size, vtbm->npages);
	entry = dttable_lookup(vtbm->dttable, blkno);

	if (!entry)
	{
		elog(NOTICE, "NOT FOUND blkno %u", blkno);
		return;
	}

	bitmap = &(vtbm->bitmap[entry->offset]);

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
