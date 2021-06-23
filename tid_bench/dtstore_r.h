#ifndef _DTSTORE_R_H
#define _DTSTORE_R_H

typedef struct DeadTupleStoreR DeadTupleStoreR;

DeadTupleStoreR *dtstore_r_create(void);
void dtstore_r_free(DeadTupleStoreR *dtstore);
void dtstore_r_add_tuples(DeadTupleStoreR *dtstore, const BlockNumber blkno,
						const OffsetNumber *offnums, int nitems);
bool dtstore_r_lookup(DeadTupleStoreR *dtstore, ItemPointer tid);
void dtstore_r_stats(DeadTupleStoreR *dtstore);
void dtstore_r_dump(DeadTupleStoreR *dtstore);
void dtstore_r_dump_blk(DeadTupleStoreR *dtstore, BlockNumber blkno);

#endif
