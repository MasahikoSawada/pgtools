#ifndef __RADIX_TREE_H__
#define __RADIX_TREE_H__

#include "postgres.h"

typedef struct radix_tree radix_tree;

extern radix_tree *radix_tree_create(MemoryContext ctx);
extern bool radix_tree_insert(radix_tree *rt, uint64 key, Datum val);
extern void radix_tree_dump(radix_tree *rt);
extern Datum radix_tree_search(radix_tree *rt, uint64 key, bool *found);
extern void radix_tree_destroy(radix_tree *tree);
extern void radix_tree_stats(radix_tree *tree);

#endif /* RADIX_TREE_H */
