/*------------------------------------------------------------------------------
 *
 * svtm.c - Specialized Vacuum TID Map
 *		Data structure to hold TIDs of dead tuples during vacuum.
 *
 * It takes in account following properties of PostgreSQL ItemPointer and
 * vacuum heap scan process:
 * - page number is 32bit integer
 * - 14 bit is enough for tuple offset.
 *   - but usually number of tuples is significantly lesser
 *   - and 0 is InvalidOffset
 * - heap is scanned sequentially therefore pages are in increasing order,
 * - tuples of a single page could be added at once.
 *
 * It uses techniques from HATM (Hash Array Mapped Trie), and Roaring bitmaps.
 *
 * # Page.
 *
 * Page information consists of 16 bit page header and bitmap or sparse bitmap
 * container. Header and bitmap contains different information
 * depending on high bits of header.
 *
 * Sparse bitmap is made from raw bitmap by skipping all-zero bytes. Non-zero
 * bytes than indexed with bitmap of sparseness.
 *
 * If bitmap contains a lot of all-one bytes, then it is inverted before
 * going to be sparse.
 *
 * Kinds of header/bitmap:
 * - embedded 1 offset
 *     high bits: 11
 *     lower bits: 14bit tuple offset
 *     bitmap: no external bitmap
 *
 * - raw bitmap
 *     high bits: 00
 *     lower bits: 14bit offset in bitmap container
 *     bitmap: 1 byte bitmap length = K
 *             K byte raw bitmap
 *   This container is used if there is no detectable pattern in offsets.
 *
 * - sparse bitmap
 *     high bits: 10
 *     lower bits: 14bit offset in bitmap container
 *     bitmap: 1 byte raw bitmap length = K
 *     		   1 byte sparseness bitmap length = S
 *     		   S bytes sparseness bitmap
 *     		   Z bytes of non-zero bitmap bytes
 *   If raw bitmap contains > 62.5% of zero bytes, then sparse bitmap format is
 *   chosen.
 *
 * - inverted sparse bitmap
 *     high bits: 10
 *     lower bits: 14bit offset in bitmap container
 *     bitmap: 1 byte raw bitmap length = K
 *     		   1 byte sparseness bitmap length = S
 *     		   S bytes sparseness bitmap
 *     		   Z bytes of non-zero inverted bitmap bytes
 *   If raw bitmap contains > 62.5% of all-ones bytes, then sparse bitmap format
 *   is used to encode whenever tuple is not dead instead.
 *
 * # Page map chunk.
 *
 * 32 consecutive page headers are stored in an sparse array together with
 * their bitmaps. Pages without any dead tuple are skipped from this array.
 *
 * Therefore chunk map contains:
 * - 32bitmap of pages presence
 * - array of 0-32 page headers
 * - byte array of concatenated bitmaps for all pages in a chunk (with offsets
 *   encoded in page headers).
 *
 * Maximum chunk size:
 * - page header map: 4 + 32*2 = 68 bytes
 * - bitmaps byte array:
 *     32kb page: 32 * 148 = 4736 byte
 *     8kb page: 32 * 36 = 1152 byte
 * - sum:
 *     32kb page: 4804 bytes
 *     8kb page: 1220 bytes
 *
 * Each chunk is allocated as a single blob.
 *
 * # Page chunk map.
 *
 * Pointers to chunks are stored into sparse array indexed with ixmap bitmap.
 * Number of first non-empty chunk and first empty chunk after it are
 * remembered to reduce size of bitmap and speedup access to first run
 * of non-empty chunks.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
#include "port/pg_bitutils.h"

#include "svtm.h"

#define PAGES_PER_CHUNK (1<<5)
#define BITMAP_PER_PAGE (MaxHeapTuplesPerPage/8 + 1)
#define PAGE_TO_CHUNK(blkno) ((uint32)(blkno)>>5)
#define CHUNK_TO_PAGE(chunkno) ((chunkno)<<5)

#define SVTAllocChunk ((1<<19)-128)

typedef struct SVTPagesChunk SVTPagesChunk;
typedef struct SVTChunkBuilder SVTChunkBuilder;
typedef struct SVTAlloc		 SVTAlloc;
typedef struct IxMap IxMap;
typedef uint16 SVTHeader;

struct SVTAlloc {
	SVTAlloc*	next;
	Size		pos;
	Size		limit;
	uint8		bytes[FLEXIBLE_ARRAY_MEMBER];
};

struct SVTChunkBuilder
{
	uint32	chunk_number;
	uint32  npages;
	uint32  bitmaps_pos;
	uint32  hcnt[4];
	BlockNumber	pages[PAGES_PER_CHUNK];
	SVTHeader	headers[PAGES_PER_CHUNK];
	/* we add 3 for BITMAP_PER_PAGE for 4 byte roundup */
	uint8   bitmaps[(BITMAP_PER_PAGE+3)*PAGES_PER_CHUNK];
};

struct IxMap {
	uint32	bitmap;
	uint32	offset;
};

struct SVTm
{
	BlockNumber	lastblock; 	/* max block number + 1 */
	struct {
		uint32	start, end;
	}			firstrun;
	uint32		nchunks;
	SVTPagesChunk **chunks; /* chunks pointers */
	IxMap	   *ixmap;   	/* compression map for chunks */
	Size		total_size;
	SVTAlloc	*alloc;

	uint32  npages;
	uint32  hcnt[4];

	SVTChunkBuilder builder; /* builder for current chunk */
};

struct SVTPagesChunk
{
	uint32  chunk_number;
	uint32 	bitmap;
	SVTHeader	headers[FLEXIBLE_ARRAY_MEMBER];
};

#define bm2(b,c) (((b)<<1)|(c))
enum SVTHeaderType {
	SVTH_rawBitmap     = bm2(0,0),
	SVTH_inverseBitmap = bm2(0,1),
	SVTH_sparseBitmap  = bm2(1,0),
	SVTH_single        = bm2(1,1),
};
#define HeaderTypeOffset (14)
#define MakeHeaderType(l) ((SVTHeader)(l) << HeaderTypeOffset)
#define HeaderType(h) (((h)>>14)&3)

#define BitmapPosition(h) ((h) & ((1<<14)-1))
#define MakeBitmapPosition(l) ((l) & ((1<<14)-1))
#define MaxBitmapPosition ((1<<14)-1)

#define SingleItem(h) ((h) & ((1<<14)-1))
#define MakeSingleItem(h) ((h) & ((1<<14)-1))

/*
 * we could not use pg_popcount32 in contrib in windows,
 * therefore define our own.
 */
#define INVALID_INDEX (~(uint32)0)
const uint8 four_bit_cnt[32] = {
	0, 1, 1, 2, 1, 2, 2, 3,
	1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4,
	2, 3, 3, 4, 3, 4, 4, 5,
};

#define makeoff(v, bits) ((v)/bits)
#define makebit(v, bits) (1<<((v)&((bits)-1)))
#define maskbits(v, vits) ((v) & ((1<<(bits))-1))
#define bitszero(v, vits) (maskbits((v), (bits)) == 0)

static inline uint32 svt_popcnt32(uint32 val);
static void svtm_build_chunk(SVTm *store);

static inline uint32
svt_popcnt8(uint8 val)
{
	return four_bit_cnt[val&15] + four_bit_cnt[(val>>4)&15];
}

static inline uint32
svt_popcnt32(uint32 val)
{
	return pg_popcount32(val);
}

static SVTAlloc*
svtm_alloc_alloc(void)
{
	SVTAlloc *alloc = palloc0(SVTAllocChunk);
	alloc->limit = SVTAllocChunk - offsetof(SVTAlloc, bytes);
	return alloc;
}

SVTm*
svtm_create(void)
{
	SVTm* store = palloc0(sizeof(SVTm));
	/* preallocate chunks just to pass it to repalloc later */
	store->chunks = palloc(sizeof(SVTPagesChunk*)*2);
	store->alloc = svtm_alloc_alloc();
	return store;
}

static void*
svtm_alloc(SVTm *store, Size size)
{
	SVTAlloc *alloc = store->alloc;
	void *res;

	size = INTALIGN(size);

	if (alloc->limit - alloc->pos < size)
	{
		alloc = svtm_alloc_alloc();
		alloc->next = store->alloc;
		store->alloc = alloc;
	}

	res = alloc->bytes + alloc->pos;
	alloc->pos += size;

	return res;
}

void
svtm_free(SVTm *store)
{
	SVTAlloc *alloc, *next;

	if (store == NULL)
		return;
	if (store->ixmap != NULL)
		pfree(store->ixmap);
	if (store->chunks != NULL)
		pfree(store->chunks);
	alloc = store->alloc;
	while (alloc != NULL)
	{
		next = alloc->next;
		pfree(alloc);
		alloc = next;
	}
	pfree(store);
}

void
svtm_add_page(SVTm *store, const BlockNumber blkno,
		const OffsetNumber *offnums, uint32 nitems)
{
	SVTChunkBuilder		*bld = &store->builder;
	SVTHeader	header = 0;
	uint32	chunkno = PAGE_TO_CHUNK(blkno);
	uint32	bmlen = 0, bbmlen = 0, bbbmlen = 0;
	uint32  sbmlen = 0;
	uint32  nonzerocnt;
	uint32  allzerocnt = 0, allonecnt = 0;
	uint32  firstoff, lastoff;
	uint32	i, j;
	uint8  *append;
	uint8	bitmap[BITMAP_PER_PAGE] = {0};
	uint8	spix1[BITMAP_PER_PAGE/8+1] = {0};
	uint8	spix2[BITMAP_PER_PAGE/64+2] = {0};
#define off(i) (offnums[i]-1)

	if (nitems == 0)
		return;

	if (chunkno != bld->chunk_number)
	{
		Assert(chunkno > bld->chunk_number);
		svtm_build_chunk(store);
		bld->chunk_number = chunkno;
	}

	Assert(bld->npages == 0 || blkno > bld->pages[bld->npages-1]);

	firstoff = off(0);
	lastoff = off(nitems-1);
	Assert(lastoff < (1<<11));

	if (nitems == 1 && lastoff < (1<<10))
	{
		/* 1 embedded item */
		header = MakeHeaderType(SVTH_single);
		header |= firstoff;
	}
	else
	{
		Assert(bld->bitmaps_pos < MaxBitmapPosition);

		append = bld->bitmaps + bld->bitmaps_pos;
		header = MakeBitmapPosition(bld->bitmaps_pos);
		/* calculate bitmap */
		for (i = 0; i < nitems; i++)
		{
			Assert(i == 0 || off(i) < off(i-1));
			bitmap[makeoff(off(i),8)] |= makebit(off(i), 8);
		}

		bmlen = lastoff/8 + 1;
		append[0] = bmlen;

		for (i = 0; i < bmlen; i++)
		{
			allzerocnt += bitmap[i] == 0;
			allonecnt += bitmap[i] == 0xff;
		}

		/* if we could not abuse sparness of bitmap, pack it as is */
		if (allzerocnt <= bmlen*5/8 && allonecnt <= bmlen*5/8)
		{
			header |= MakeHeaderType(SVTH_rawBitmap);
			memmove(append+1, bitmap, bmlen);
			bld->bitmaps_pos += bmlen + 1;
		}
		else
		{
			/* if there is more present tuples than absent, invert map */
			if (allonecnt > bmlen*5/8)
			{
				header |= MakeHeaderType(SVTH_inverseBitmap);
				for (i = 0; i < bmlen; i++)
					bitmap[i] ^= 0xff;
				nonzerocnt = bmlen - allonecnt;
			}
			else
			{
				header |= MakeHeaderType(SVTH_sparseBitmap);
				nonzerocnt = bmlen - allzerocnt;
			}

			/* Then we compose two level bitmap index for bitmap. */

			/* First compress bitmap itself with first level index */
			bbmlen = (bmlen+7)/8;
			j = 0;
			for (i = 0; i < bmlen; i++)
			{
				if (bitmap[i] != 0)
				{
					spix1[makeoff(i, 8)] |= makebit(i, 8);
					bitmap[j] = bitmap[i];
					j++;
				}
			}
			Assert(j == nonzerocnt);

			/* Then compress first level index with second level */
			bbbmlen = (bbmlen+7)/8;
			Assert(bbbmlen <= 3);
			sbmlen = 0;
			for (i = 0; i < bbmlen; i++)
			{
				if (spix1[i] != 0)
				{
					spix2[makeoff(i, 8)] |= makebit(i, 8);
					spix1[sbmlen] = spix1[i];
					sbmlen++;
				}
			}
			Assert(sbmlen < 19);

			/*
			 * second byte contains length of first level and offset
			 * to compressed bitmap itself.
			 */
			append[1] = (bbbmlen << 5) | (bbbmlen + sbmlen);
			memmove(append+2, spix2, bbbmlen);
			memmove(append+2+bbbmlen, spix1, sbmlen);
			memmove(append+2+bbbmlen+sbmlen, bitmap, nonzerocnt);
			bld->bitmaps_pos += bbbmlen + sbmlen + nonzerocnt + 2;
		}
		Assert(bld->bitmaps_pos <= MaxBitmapPosition);
	}
	bld->pages[bld->npages] = blkno;
	bld->headers[bld->npages] = header;
	bld->npages++;
	bld->hcnt[HeaderType(header)]++;
}
#undef off

static void
svtm_build_chunk(SVTm *store)
{
	SVTChunkBuilder		*bld = &store->builder;
	SVTPagesChunk	*chunk;
	uint32 bitmap = 0;
	BlockNumber startblock;
	uint32 off;
	uint32 i;
	Size total_size;

	Assert(bld->npages < ~(uint16)0);

	if (bld->npages == 0)
		return;

	startblock = CHUNK_TO_PAGE(bld->chunk_number);
	for (i = 0; i < bld->npages; i++)
	{
		off = bld->pages[i] - startblock;
		bitmap |= makebit(off, 32);
	}

	total_size = offsetof(SVTPagesChunk, headers) +
		sizeof(SVTHeader)*bld->npages +
		bld->bitmaps_pos;

	chunk = svtm_alloc(store, total_size);
	chunk->chunk_number = bld->chunk_number;;
	chunk->bitmap = bitmap;
	memmove(chunk->headers,
			bld->headers, sizeof(SVTHeader)*bld->npages);
	memmove((char*)(chunk->headers + bld->npages),
			bld->bitmaps, bld->bitmaps_pos);

	/*
	 * We allocate store->chunks in power-of-two sizes.
	 * Then check for "we will overflow" is equal to "nchunks is power of two".
	 */
	if ((store->nchunks & (store->nchunks-1)) == 0)
	{
		Size new_nchunks = store->nchunks ? (store->nchunks<<1) : 1;
		store->chunks = (SVTPagesChunk**) repalloc(store->chunks,
				new_nchunks * sizeof(SVTPagesChunk*));
	}
	store->chunks[store->nchunks] = chunk;
	store->nchunks++;
	store->lastblock = bld->pages[bld->npages-1];
	store->total_size += total_size;

	for (i = 0; i<4; i++)
		store->hcnt[i] += bld->hcnt[i];
	store->npages += bld->npages;

	memset(bld, 0, sizeof(SVTChunkBuilder));
}

void
svtm_finalize_addition(SVTm *store)
{
	SVTPagesChunk **chunks = store->chunks;
	IxMap  *ixmap;
	uint32	last_chunk, chunkno;
	uint32	firstrun, firstrunend;
	uint32	nmaps;
	uint32	i;

	if (store->nchunks == 0)
	{
		/*
		 * block number will be rejected with:
		 * block <= lastblock, lastblock == 0
		 * chunk >= firstrun.start, firstrun.start = 1
		 */
		store->firstrun.start = 1;
		return;
	}

	firstrun = chunks[0]->chunk_number;
	firstrunend = firstrun+1;

	/* adsorb last chunk */
	svtm_build_chunk(store);

	/* Now we need to build ixmap */
	last_chunk = PAGE_TO_CHUNK(store->lastblock);
	nmaps = makeoff(last_chunk, 32) + 1;
	ixmap = palloc0(nmaps * sizeof(IxMap));

	for (i = 0; i < store->nchunks; i++)
	{
		chunkno = chunks[i]->chunk_number;
		if (chunkno == firstrunend)
			firstrunend++;
		chunkno -= firstrun;
		ixmap[makeoff(chunkno,32)].bitmap |= makebit(chunkno,32);
	}

	for (i = 1; i < nmaps; i++)
	{
		ixmap[i].offset = ixmap[i-1].offset;
		ixmap[i].offset += svt_popcnt32(ixmap[i-1].bitmap);
	}

	store->firstrun.start = firstrun;
	store->firstrun.end = firstrunend;
	store->ixmap = ixmap;
}

bool
svtm_lookup(SVTm *store, ItemPointer tid)
{
	BlockNumber		blkno = ItemPointerGetBlockNumber(tid);
	OffsetNumber	offset = ItemPointerGetOffsetNumber(tid) - 1;
	SVTPagesChunk  *chunk;
	IxMap          *ixmap = store->ixmap;
	uint32			off, bit;

	SVTHeader		header;
	uint8		   *bitmaps;
	uint8		   *bitmap;
	uint32	index;
	uint32	chunkno, blk_in_chunk;
	uint8	type;
	uint8	bmoff, bmbit, bmlen, bmbyte;
	uint8	bmstart, bbmoff, bbmbit, bbmbyte;
	uint8	bbbmlen, bbbmoff, bbbmbit;
	uint8	six1off, sbmoff;
	bool	inverse, bitset;

	if (blkno > store->lastblock)
		return false;

	chunkno = PAGE_TO_CHUNK(blkno);
	if (chunkno < store->firstrun.start)
		return false;

	if (chunkno < store->firstrun.end)
		index = chunkno - store->firstrun.start;
	else
	{
		off = makeoff(chunkno - store->firstrun.start, 32);
		bit = makebit(chunkno - store->firstrun.start, 32);
		if ((ixmap[off].bitmap & bit) == 0)
			return false;

		index = ixmap[off].offset + svt_popcnt32(ixmap[off].bitmap & (bit-1));
	}
	chunk = store->chunks[index];
	Assert(chunkno == chunk->chunk_number);

	blk_in_chunk = blkno - CHUNK_TO_PAGE(chunkno);
	bit = makebit(blk_in_chunk, 32);

	if ((chunk->bitmap & bit) == 0)
		return false;
	index = svt_popcnt32(chunk->bitmap & (bit - 1));
	header = chunk->headers[index];

	type = HeaderType(header);
	if (type == SVTH_single)
		return offset == SingleItem(header);

	bitmaps = (uint8*)(chunk->headers + svt_popcnt32(chunk->bitmap));
	bmoff = makeoff(offset, 8);
	bmbit = makebit(offset, 8);
	inverse = false;

	bitmap = bitmaps + BitmapPosition(header);
	bmlen = bitmap[0];
	if (bmoff >= bmlen)
		return false;

	switch (type)
	{
		case SVTH_rawBitmap:
			return (bitmap[bmoff+1] & bmbit) != 0;

		case SVTH_inverseBitmap:
			inverse = true;
			/* fallthrough */
		case SVTH_sparseBitmap:
			bmstart = bitmap[1] & 0x1f;
			bbbmlen = bitmap[1] >> 5;
			bitmap += 2;
			bbmoff = makeoff(bmoff, 8);
			bbmbit = makebit(bmoff, 8);
			bbbmoff = makeoff(bbmoff, 8);
			bbbmbit = makebit(bbmoff, 8);
			/* check bit in second level index */
			if ((bitmap[bbbmoff] & bbbmbit) == 0)
				return inverse;
			/* calculate sparse offset into compressed first level index */
			six1off = pg_popcount((char*)bitmap, bbbmoff) +
						svt_popcnt8(bitmap[bbbmoff] & (bbbmbit-1));
			/* check bit in first level index */
			bbmbyte = bitmap[bbbmlen+six1off];
			if ((bbmbyte & bbmbit) == 0)
				return inverse;
			/* and sparse offset into compressed bitmap itself */
			sbmoff = pg_popcount((char*)bitmap+bbbmlen, six1off) +
						svt_popcnt8(bbmbyte & (bbmbit-1));
			bmbyte = bitmap[bmstart + sbmoff];
			/* finally check bit in bitmap */
			bitset = (bmbyte & bmbit) != 0;
			return bitset != inverse;
	}
	Assert(false);
	return false;
}

void svtm_stats(SVTm *store)
{
	StringInfo s;

	s = makeStringInfo();
	appendStringInfo(s, "svtm: nchunks %u npages %u\n",
			store->nchunks, store->npages);
	appendStringInfo(s, "single=%u raw=%u inserse=%u sparse=%u",
			store->hcnt[SVTH_single], store->hcnt[SVTH_rawBitmap],
			store->hcnt[SVTH_inverseBitmap], store->hcnt[SVTH_sparseBitmap]);

	elog(NOTICE, "%s", s->data);
	pfree(s->data);
	pfree(s);
}
