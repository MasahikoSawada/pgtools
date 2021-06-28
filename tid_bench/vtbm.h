#ifndef _VTBM_H
#define _VTBM_H

typedef struct VTbm VTbm;

VTbm *vtbm_create(void);
void vtbm_free(VTbm *vtbm);
void vtbm_add_tuples(VTbm *vtbm, const BlockNumber blkno,
						const OffsetNumber *offnums, int nitems);
bool vtbm_lookup(VTbm *vtbm, ItemPointer tid);
void vtbm_stats(VTbm *vtbm);
void vtbm_dump(VTbm *vtbm);
void vtbm_dump_blk(VTbm *vtbm, BlockNumber blkno);

#endif
