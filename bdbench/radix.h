/*-------------------------------------------------------------------------
 *
 * radix.h
 *	  radix tree, yay.
 *
 *
 * Portions Copyright (c) 2014-2021, PostgreSQL Global Development Group
 *
 * src/include/storage/radix.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef RADIX_H
#define RADIX_H

typedef uint64 bfm_key_type;
typedef uint64 bfm_value_type;
//typedef uint32 bfm_value_type;
//typedef char bfm_value_type;

/* How many different size classes are there */
#define BFM_KIND_COUNT 6

typedef enum bfm_tree_node_kind
{
	BFM_KIND_1,
	BFM_KIND_4,
	BFM_KIND_16,
	BFM_KIND_32,
	BFM_KIND_128,
	BFM_KIND_MAX
} bfm_tree_node_kind;

struct MemoryContextData;
struct bfm_tree_node;

/* NB: makes things a bit slower */
#define BFM_STATS

#define BFM_USE_SLAB
//#define BFM_USE_OS

/*
 * A radix tree with nodes that are sized based on occupancy.
 */
typedef struct bfm_tree
{
	struct bfm_tree_node *rnode;
	uint64 maxval;

	struct MemoryContextData *context;
#ifdef BFM_USE_SLAB
	struct MemoryContextData *inner_slabs[BFM_KIND_COUNT];
	struct MemoryContextData *leaf_slabs[BFM_KIND_COUNT];
#endif

#ifdef BFM_STATS
	/* stats */
	size_t entries;
	size_t inner_nodes[BFM_KIND_COUNT];
	size_t leaf_nodes[BFM_KIND_COUNT];
#endif
} bfm_tree;

extern void bfm_init(bfm_tree *root);
extern bool bfm_lookup(bfm_tree *root, bfm_key_type key, bfm_value_type *val);
extern bool bfm_set(bfm_tree *root, bfm_key_type key, bfm_value_type val);
extern bool bfm_delete(bfm_tree *root, bfm_key_type key);

extern struct StringInfoData* bfm_stats(bfm_tree *root);
extern void bfm_print(bfm_tree *root);

extern void bfm_tests(void);

#endif
