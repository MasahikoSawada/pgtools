/*-------------------------------------------------------------------------
 *
 * rtbm.c (Vacuum TID Map)
 *		Data structure to hold TIDs of dead tuples during vacuum.
 *
 * Vacuum TID map is used to hold the TIDs of dead tuple during lazy vacuum.
 * This module is based on the paper "Consistently faster and smaller
 * compressed bitmaps with Roaring" by D. Lemire, G. Ssi-Yan-Kai, O. Kaser,
 * arXiv:1603.06549, [cs.DB], 2 Mar 2018.
 *
 * The authors provide an implementation of Roaring Bitmap at
 * https://github.com/RoaringBitmap/CRoaring (Apache 2.0 license). Even if
 * it were practical to use this library directly, we cannot because we need
 * to integrate it with Dynamic Shared Memory/Area to support parallel vacuum
 * and need to support ItemPointerData, 6-byte integer in total, whereas the
 * implementation supports only 4-byte integers. Also, for vacuum purpose,
 * we neither need to compute the intersection, the union, nor the difference
 * between sets, but need only an existence check.
 *
 * Therefore, this code was written from scratch while begin inspired by the
 * idea of Roaring Bitmap.  The data structure is similar to TIDBitmap with some
 * exceptions.  It consists of a hash table managing entries per block and a
 * variable-lenth array that is commonly used by block entries. Each block
 * entry has its container storing offsets of dead tuples and its length. There
 * are three types of container: array, bitmap, and run.

 * - An array container is object containing 2-byte integers representing offset
 * numbers. The corresponding block entry has the number of 2-byte integers
 * (OffsetNumber) as its length.
 *
 * - A bitmap container is an object representing an uncompressed bitmap, able
 * to store all OffsetNumbers at maximum. The corresponding block entry has the
 * number of bits as its length.
 *
 * - A run container is an object made of an array of pairs of 2-byte integers.
 * The first value of each pair represents a starting OffsetNumber whereas the
 * second value represents the length.  The corresponding block entry has the
 * number of 2-byte integers as its length.
 *
 * The smallest container type varies depending on the cardinarity of the offset
 * numbers in the block.
 *
 * Limitations
 * -----------
 * - No support for removing and updating block and offset values.
 *
 * - Offset numbers must be added in order.
 *
 * - No computation of the intersection, the union, and the difference of
 *   between sets.
 *
 * TODO
 * ----
 * - Support iteration.
 *
 * - Support DSM and DSA.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/vacuumtidmap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "rtbm.h"

/*
 * The lowest 4 bits are used by the type of the container. The remainig
 * 12 bits represent the length. 12 bits is enough for the the number of
 * elements in the container. In the array container case, we need
 * MaxHeapTuplePerPage (typically 256 with 8K pages, or 1024 with 32K pages)
 * 2-byte integers in the array container, in the bitmap container case,
 * we need MaxHeapTuplePerPage bits, and in the run container case, we
 * need MaxHeapTuplePerPage pairs of 2-byte integers.
 */
#define DTENTRY_FLAG_TYPE_ARRAY	0x1000
#define DTENTRY_FLAG_TYPE_BITMAP	0x2000
#define DTENTRY_FLAG_TYPE_RUN		0x4000
#define DTENTRY_FLAG_NUM_MASK		0x0FFF

typedef struct DtEntry
{
	BlockNumber blkno; /* block number (hash table key) */
	char		status; /* hash entry status */
	uint16		flags; /* type of the container and the number of elements
						* in the container */
	uint64		offset; /* start offset within the containerdata */
} DtEntry;

#define DTENTRY_IS_ARRAY(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAG_TYPE_ARRAY) != 0)
#define DTENTRY_IS_BITMAP(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAG_TYPE_BITMAP) != 0)
#define DTENTRY_IS_RUN(entry) \
	((((DtEntry *) (entry))->flags & DTENTRY_FLAG_TYPE_RUN) != 0)

#define BITMAP_CONTAINER_SIZE(maxoff) (((maxoff) - 1) / BITBYTE + 1)
#define MAX_BITMAP_CONTAINER_SIZE BITMAP_CONTAINER_SIZE(MaxHeapTuplesPerPage)

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

	int		nblocks;

	char	*containerdata;	/* the space for containers */
	uint64	containerdata_size;
	uint32	offset;	/* current offset within the containerdata */
} RTbm;
#define RTBM_CONTAINERDATA_INITIAL_SIZE	(64 * 1024) /* 64kB */

#define BITBYTE 8
#define BYTENUM(x) ((x) / BITBYTE)
#define BITNUM(x) ((x) % BITBYTE)

static void enlarge_container_space(RTbm *rtbm)
{
	int newsize = rtbm->containerdata_size * 2;

	rtbm->containerdata = repalloc_huge(rtbm->containerdata, newsize);
	rtbm->containerdata_size = newsize;
}

RTbm *
rtbm_create(void)
{
	RTbm *rtbm = palloc0(sizeof(RTbm));

	rtbm->dttable = dttable_create(CurrentMemoryContext, 128, (void *) rtbm);

	rtbm->containerdata = palloc0(RTBM_CONTAINERDATA_INITIAL_SIZE);
	rtbm->containerdata_size = RTBM_CONTAINERDATA_INITIAL_SIZE;

	return rtbm;
}

void
rtbm_free(RTbm *rtbm)
{
	pfree(rtbm->containerdata);
	pfree(rtbm);
}

/*
 * Create a run container into *container. Return the container
 * size in bytes.
 */
static int
create_run_container(const OffsetNumber *offnums, int noffs, uint16 *container)
{
	OffsetNumber startoff;
	uint16	length;
	int		coffset = 0;

	for (int i = 1; i <= noffs; i++)
	{
		length = 1;
		startoff = offnums[i - 1];

		while (i <= noffs && offnums[i - 1] + 1 == offnums[i])
		{
			length++;
			i++;
		}

		container[coffset++] = startoff;
		container[coffset++] = length;
	}

	return (int) (coffset * sizeof(uint16));
}

/*
 * Choose the smallest size of container type. We cannot know how big the run
 * container is without actual creation. So this function also creates the
 * run container in *runcontainer.
 */
static int
choose_container_type(const OffsetNumber *offnums, int noffs, int *size_p,
					  uint16 *runcontainer)
{
	int		array_size;
	int 	bitmap_size;
	int		run_size;

	/* calculate the space needed by each container type in byte */
	array_size = noffs * sizeof(OffsetNumber);
	bitmap_size = BITMAP_CONTAINER_SIZE(offnums[noffs - 1]);
	run_size = create_run_container(offnums, noffs, runcontainer);

	if (bitmap_size <= array_size && bitmap_size <= run_size)
	{
		*size_p = bitmap_size;
		return DTENTRY_FLAG_TYPE_BITMAP;
	}
	else if (run_size < bitmap_size && run_size < array_size)
	{
		*size_p = run_size;
		return DTENTRY_FLAG_TYPE_RUN;
	}
	else
	{
		*size_p = array_size;
		return DTENTRY_FLAG_TYPE_ARRAY;
	}
}

void
rtbm_add_tuples(RTbm *rtbm, const BlockNumber blkno,
				   const OffsetNumber *offnums, int nitems)
{
	DtEntry *entry;
	bool	found;
	char	oldstatus;
	OffsetNumber runcontainer[MaxHeapTuplesPerPage];
	int container_type;
	int container_size;

	entry = dttable_insert(rtbm->dttable, blkno, &found);
	Assert(!found);

	/* initialize entry */
	oldstatus = entry->status;
	MemSet(entry, 0, sizeof(DtEntry));
	entry->status = oldstatus;
	entry->blkno = blkno;
	entry->offset = rtbm->offset;

	/* Choose smallest container type */
	container_type = choose_container_type(offnums, nitems, &container_size,
										   runcontainer);
	/* Make sure we have enough container data space */
	if ((rtbm->offset + container_size) > rtbm->containerdata_size)
	{
		enlarge_container_space(rtbm);
		Assert((rtbm->offset + container_size) < rtbm->containerdata_size);
	}

	if (container_type == DTENTRY_FLAG_TYPE_BITMAP)
	{
		for (int i = 0; i < nitems; i++)
		{
			OffsetNumber off = offnums[i];
			int	bytenum;
			int	bitnum;

			bytenum = BYTENUM(off - 1);
			bitnum = BITNUM(off - 1);

			rtbm->containerdata[entry->offset + bytenum] |= (1 << bitnum);
		}

		entry->flags |= DTENTRY_FLAG_TYPE_BITMAP;

		/* Bitmap container has the number of bits */
		entry->flags |= ((container_size * BITBYTE) & DTENTRY_FLAG_NUM_MASK);
	}
	else if (container_type == DTENTRY_FLAG_TYPE_RUN)
	{
		uint16 nruns = container_size / sizeof(OffsetNumber);

		/* Copy already-created run container */
		memcpy(&(rtbm->containerdata[entry->offset]), runcontainer,
			   container_size);

		entry->flags |= DTENTRY_FLAG_TYPE_RUN;

		/* Bitmap container has the number of 2-byte integers */
		entry->flags |= (((uint16) nruns) & DTENTRY_FLAG_NUM_MASK);
	}
	else
	{
		/* An array container has the simple array of OffsetNumber */
		memcpy(&(rtbm->containerdata[entry->offset]), offnums,
			   sizeof(OffsetNumber) * nitems);

		entry->flags |= DTENTRY_FLAG_TYPE_ARRAY;

		/* Array containers have the number of OffsetNumbers */
		entry->flags |= (((uint16) nitems) & DTENTRY_FLAG_NUM_MASK);
	}

	rtbm->offset += container_size;
	rtbm->nblocks++;
}

bool
rtbm_lookup(RTbm *rtbm, ItemPointer tid)
{
	BlockNumber blk = ItemPointerGetBlockNumber(tid);
	OffsetNumber off = ItemPointerGetOffsetNumber(tid);
	DtEntry *entry;
	int bytenum, bitnum;
	bool ret = false;
	uint16 len;

	entry = dttable_lookup(rtbm->dttable, blk);

	if (!entry)
		return false;

	len = (uint16) (entry->flags & DTENTRY_FLAG_NUM_MASK);

	if (DTENTRY_IS_ARRAY(entry))
	{
		OffsetNumber *off_p = (OffsetNumber *) &(rtbm->containerdata[entry->offset]);
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
			bytenum = BYTENUM(off - 1);
			bitnum = BITNUM(off - 1);

			ret = ((rtbm->containerdata[entry->offset + bytenum] & (1 << bitnum)) != 0);
		}
	}
	else
	{
		OffsetNumber *runs = (OffsetNumber *) &(rtbm->containerdata[entry->offset]);

		for (int i = 0; i < len; i += 2)
		{
			OffsetNumber start = runs[i];
			uint16 end = start + runs[i + 1] - 1;

			if (off < start)
			{
				ret = false;
				break;
			}

			if (off > end)
				continue;

			ret = true;
			break;
		}
	}

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

void
rtbm_stats(RTbm *rtbm)
{
	elog(NOTICE, "dtatble_size %d containerdata_size %lu nblocks %d, offset %d",
		 rtbm->dttable_size,
		 rtbm->containerdata_size,
		 rtbm->nblocks,
		 rtbm->offset);
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

	len = (uint16) (entry->flags & DTENTRY_FLAG_NUM_MASK);

	if (DTENTRY_IS_ARRAY(entry))
	{
		OffsetNumber *off_p = (OffsetNumber *) &(rtbm->containerdata[entry->offset]);

		for (int i = 0; i < len; i++)
			appendStringInfo(&str, "%d ", *(off_p + i));
	}
	else if (DTENTRY_IS_BITMAP(entry))
	{
		char *container = &(rtbm->containerdata[entry->offset]);

		for (int off = 0; off < len; off++)
		{
			int bytenum = BYTENUM(off - 1), bitnum = BITNUM(off - 1);

			appendStringInfo(&str, "%s",
							 ((container[bytenum] & ((char) 1 << bitnum)) != 0) ? "1" : "0");
			if (off > 0 && off % 8 == 0)
				appendStringInfo(&str, "%s", " ");
		}
	}
	else
	{
		OffsetNumber *runcontainer = (OffsetNumber *) &(rtbm->containerdata[entry->offset]);

		for (int i = 0; i < len; i += 2)
		{
			appendStringInfo(&str, "[%d:%d] ",
							 (uint16) runcontainer[i],
							 (uint16) runcontainer[i + 1]);
		}
	}

	elog(NOTICE, "%s (offset %llu len %d)",
		 str.data,
		 (long long unsigned) entry->offset, len);
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
rtbm_dump(RTbm *rtbm)
{
	dttable_iterator iter;
	DtEntry *entry;
	DtEntry **entries;
	int num = 0;

	entries = (DtEntry **) palloc(rtbm->nblocks * sizeof(DtEntry *));

	dttable_start_iterate(rtbm->dttable, &iter);
	while ((entry = dttable_iterate(rtbm->dttable, &iter)) != NULL)
		entries[num++] = entry;

	qsort(entries, rtbm->nblocks, sizeof(DtEntry *), rtbm_comparator);

	elog(NOTICE, "DEADTUPLESTORE (containerdata size %lu, nblocks %d) ----------------------------",
		 rtbm->containerdata_size, rtbm->nblocks);
	for (int i = 0; i < rtbm->nblocks; i++)
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

	elog(NOTICE, "DEADTUPLESTORE (containerdata size %lu, nblocks %d) ----------------------------",
		 rtbm->containerdata_size, rtbm->nblocks);
	entry = dttable_lookup(rtbm->dttable, blkno);

	if (!entry)
	{
		elog(NOTICE, "NOT FOUND blkno %u", blkno);
		return;
	}

	dump_entry(rtbm, entry);
}
