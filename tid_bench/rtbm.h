#ifndef _RTBM_H
#define _RTBM_H

typedef struct RTbm RTbm;

RTbm *rtbm_create(void);
void rtbm_free(RTbm *dtstore);
void rtbm_add_tuples(RTbm *dtstore, const BlockNumber blkno,
						const OffsetNumber *offnums, int nitems);
bool rtbm_lookup(RTbm *dtstore, ItemPointer tid);
void rtbm_stats(RTbm *dtstore);
void rtbm_dump(RTbm *dtstore);
void rtbm_dump_blk(RTbm *dtstore, BlockNumber blkno);

#endif
