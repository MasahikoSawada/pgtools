#ifndef _SVTM_H
#define _SVTM_H

/* Specialized Vacuum TID Map */
typedef struct SVTm SVTm;

SVTm *svtm_create(void);
void svtm_free(SVTm *store);
/*
 * Add page tuple offsets to map.
 * offnums should be sorted. Max offset number should be < 2048.
 */
void svtm_add_page(SVTm *store, const BlockNumber blkno,
		const OffsetNumber *offnums, uint32 nitems);
void svtm_finalize_addition(SVTm *store);
bool svtm_lookup(SVTm *store, ItemPointer tid);
void svtm_stats(SVTm *store);

#endif
