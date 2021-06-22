#ifndef _LVDEAD_TUPLE_H
#define _LVDEAD_TUPLE_H

typedef struct DeadTupleStore DeadTupleStore;

DeadTupleStore *dtstore_create(void);
void dtstore_free(DeadTupleStore *dtstore);
void dtstore_add_tuples(DeadTupleStore *dtstore, const BlockNumber blkno,
						const OffsetNumber *offnums, int nitems);
bool dtstore_lookup(DeadTupleStore *dtstore, ItemPointer tid);
void dtstore_stats(DeadTupleStore *dtstore);
void dtstore_dump(DeadTupleStore *dtstore);
void dtstore_dump_blk(DeadTupleStore *dtstore, BlockNumber blkno);

#endif
