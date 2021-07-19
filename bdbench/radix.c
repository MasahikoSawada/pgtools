/*
 *
 */

#include "postgres.h"

#include "radix.h"

#include "lib/stringinfo.h"
#include "port/pg_bitutils.h"
#include "utils/memutils.h"


/*
 * How many bits are encoded in one tree level.
 *
 * Linux uses 6, ART uses 8. In a non-adaptive radix tree the disadvantage of
 * a higher fanout is increased memory usage - but the adapative node size
 * addresses that to a good degree. Using a common multiple of 8 (i.e. bytes
 * in a byte) has the advantage of making it easier to eventually support
 * variable length data. Therefore go with 8 for now.
 */
#define BFM_FANOUT			8

#define BFM_MAX_CLASS		(1<<BFM_FANOUT)

#define BFM_MASK			((1 << BFM_FANOUT) - 1)


/*
 * Base type for all node types.
 */
struct bfm_tree_node_inner;
typedef struct bfm_tree_node
{
	/*
	 * Size class of entry (stored as uint8 instead of bfm_tree_node_kind to
	 * save space).
	 *
	 * XXX: For efficiency in random access cases it'd be a good idea to
	 * encode the kind of a node in the pointer value of upper nodes, in the
	 * low bits. Being able to do the node type dispatch during traversal
	 * before the memory for the node has been fetched from memory would
	 * likely improve performance significantly.  But that'd require at least
	 * 8 byte alignment, which we don't currently guarantee on all platforms.
	 */
	uint8 kind;

	/*
	 * Shift indicates which part of the key space is represented by this
	 * node. I.e. the key is shifted by `shift` and the lowest BFM_FANOUT bits
	 * are then represented in chunk.
	 */
	uint8 node_shift;
	uint8 node_chunk;

	/*
	 * Number of children - currently uint16 to be able to indicate 256
	 * children at a fanout of 8.
	 */
	uint16 count;

	/* FIXME: Right now there's always unused bytes here :( */

	/*
	 * FIXME: could be removed by using a stack while walking down to deleted
	 * node.
	 */
	struct bfm_tree_node_inner *parent;
} bfm_tree_node;

/*
 * Base type for all inner nodes.
 */
typedef struct bfm_tree_node_inner
{
	bfm_tree_node b;
} bfm_tree_node_inner;

/*
 * Base type for all leaf nodes.
 */
typedef struct bfm_tree_node_leaf
{
	bfm_tree_node b;
} bfm_tree_node_leaf;


/*
 * Size classes.
 *
 * To reduce memory usage compared to a simple radix tree with a fixed fanout
 * we use adaptive node sizes, with different storage methods for different
 * numbers of elements.
 *
 * FIXME: These are currently not well chosen. To reduce memory fragmentation
 * smaller class should optimally fit neatly into the next larger class
 * (except perhaps at the lowest end). Right now its
 * 32->56->160->304->1296->2064/2096 bytes for inner/leaf nodes, repeatedly
 * just above a power of 2, leading to large amounts of allocator padding with
 * aset.c. Hence the use of slab.
 *
 * FIXME: Duplication.
 *
 * XXX: Consider implementing path compression, it reduces worst case memory
 * usage substantially. I.e. collapse sequences of nodes with just one child
 * into one node. That would make it feasible to use this datastructure for
 * wide keys. Gut feeling: When compressing inner nodes a limited number of
 * tree levels should be skippable to keep nodes of a constant size. But when
 * collapsing to leaf nodes it likely is worth to make them variable width,
 * it's such a common scenario (a sparse key will always end with such a chain
 * of nodes).
 */

/*
 * Inner node size classes.
 */
typedef struct bfm_tree_node_inner_1
{
	bfm_tree_node_inner b;

	/* single child, for key chunk */
	uint8 chunk;
	bfm_tree_node *slot;
} bfm_tree_node_inner_1;

typedef struct bfm_tree_node_inner_4
{
	bfm_tree_node_inner b;

	/* four children, for key chunks */
	uint8 chunks[4];
	bfm_tree_node *slots[4];
} bfm_tree_node_inner_4;

typedef struct bfm_tree_node_inner_16
{
	bfm_tree_node_inner b;

	/* four children, for key chunks */
	uint8 chunks[16];
	bfm_tree_node *slots[16];
} bfm_tree_node_inner_16;

#define BFM_TREE_NODE_32_INVALID 0xFF
typedef struct bfm_tree_node_inner_32
{
	bfm_tree_node_inner b;

	/*
	 * 32 children. Offsets is indexed by they key chunk and points into
	 * ->slots. An offset of BFM_TREE_NODE_32_INVALID indicates a non-existing
	 * entry.
	 *
	 * XXX: It'd be nice to shrink the offsets array to use fewer bits - we
	 * only need to index into an array of 32 entries. But 32 offsets already
	 * is 5 bits, making a simple & fast encoding nontrivial.
	 */
	uint8 chunks[32];
	bfm_tree_node *slots[32];
} bfm_tree_node_inner_32;

#define BFM_TREE_NODE_128_INVALID 0xFF
typedef struct bfm_tree_node_inner_128
{
	bfm_tree_node_inner b;

	uint8 offsets[BFM_MAX_CLASS];
	bfm_tree_node *slots[128];
} bfm_tree_node_inner_128;

typedef struct bfm_tree_node_inner_max
{
	bfm_tree_node_inner b;
	bfm_tree_node *slots[BFM_MAX_CLASS];
} bfm_tree_node_inner_max;


/*
 * Leaf node size classes.
 *
 * Currently these are separate from inner node size classes for two main
 * reasons:
 *
 * 1) the value type might be different than something fitting into a pointer
 *    width type
 * 2) Need to represent non-existing values in a key-type independent way.
 *
 * 1) is clearly worth being concerned about, but it's not clear 2) is as
 * good. It might be better to just indicate non-existing entries the same way
 * in inner nodes.
 */

typedef struct bfm_tree_node_leaf_1
{
	bfm_tree_node_leaf b;
	uint8 chunk;
	bfm_value_type value;
} bfm_tree_node_leaf_1;

#define BFM_TREE_NODE_LEAF_4_INVALID 0xFFFF
typedef struct bfm_tree_node_leaf_4
{
	bfm_tree_node_leaf b;
	uint8 chunks[4];
	bfm_value_type values[4];
} bfm_tree_node_leaf_4;

#define BFM_TREE_NODE_LEAF_16_INVALID 0xFFFF
typedef struct bfm_tree_node_leaf_16
{
	bfm_tree_node_leaf b;
	uint8 chunks[16];
	bfm_value_type values[16];
} bfm_tree_node_leaf_16;

typedef struct bfm_tree_node_leaf_32
{
	bfm_tree_node_leaf b;
	uint8 chunks[32];
	bfm_value_type values[32];
} bfm_tree_node_leaf_32;

typedef struct bfm_tree_node_leaf_128
{
	bfm_tree_node_leaf b;
	uint8 offsets[BFM_MAX_CLASS];
	bfm_value_type values[128];
} bfm_tree_node_leaf_128;

typedef struct bfm_tree_node_leaf_max
{
	bfm_tree_node_leaf b;
	uint8 set[BFM_MAX_CLASS / (sizeof(uint8) * BITS_PER_BYTE)];
	bfm_value_type values[BFM_MAX_CLASS];
} bfm_tree_node_leaf_max;


typedef struct bfm_tree_size_class_info
{
	const char *const name;
	int elements;
	size_t size;
} bfm_tree_size_class_info;

const bfm_tree_size_class_info inner_class_info[] =
{
	[BFM_KIND_1] = {"1", 1, sizeof(bfm_tree_node_inner_1)},
	[BFM_KIND_4] = {"4", 4, sizeof(bfm_tree_node_inner_4)},
	[BFM_KIND_16] = {"16", 16, sizeof(bfm_tree_node_inner_16)},
	[BFM_KIND_32] = {"32", 32, sizeof(bfm_tree_node_inner_32)},
	[BFM_KIND_128] = {"128", 128, sizeof(bfm_tree_node_inner_128)},
	[BFM_KIND_MAX] = {"max", BFM_MAX_CLASS, sizeof(bfm_tree_node_inner_max)},
};

const bfm_tree_size_class_info leaf_class_info[] =
{
	[BFM_KIND_1] = {"1", 1, sizeof(bfm_tree_node_leaf_1)},
	[BFM_KIND_4] = {"4", 4, sizeof(bfm_tree_node_leaf_4)},
	[BFM_KIND_16] = {"16", 16, sizeof(bfm_tree_node_leaf_16)},
	[BFM_KIND_32] = {"32", 32, sizeof(bfm_tree_node_leaf_32)},
	[BFM_KIND_128] = {"128", 128, sizeof(bfm_tree_node_leaf_128)},
	[BFM_KIND_MAX] = {"max", BFM_MAX_CLASS, sizeof(bfm_tree_node_leaf_max)},
};

static void *
bfm_alloc_node(bfm_tree *root, bool inner, bfm_tree_node_kind kind, size_t size)
{
	bfm_tree_node *node;

#ifdef BFM_USE_SLAB
	if (inner)
		node = (bfm_tree_node *) MemoryContextAlloc(root->inner_slabs[kind], size);
	else
		node = (bfm_tree_node *) MemoryContextAlloc(root->leaf_slabs[kind], size);
#elif defined(BFM_USE_OS)
	node = (bfm_tree_node *) malloc(size);
#else
	node = (bfm_tree_node *) MemoryContextAlloc(root->context, size);
#endif

	return node;
}

static bfm_tree_node_inner *
bfm_alloc_inner(bfm_tree *root, bfm_tree_node_kind kind, size_t size)
{
	bfm_tree_node_inner *node;

	Assert(inner_class_info[kind].size == size);
#ifdef BFM_STATS
	root->inner_nodes[kind]++;
#endif

	node = bfm_alloc_node(root, true, kind, size);

	memset(&node->b, 0, sizeof(node->b));
	node->b.kind = kind;

	return node;
}

static bfm_tree_node_inner *
bfm_alloc_leaf(bfm_tree *root, bfm_tree_node_kind kind, size_t size)
{
	bfm_tree_node_inner *node;

	Assert(leaf_class_info[kind].size == size);
#ifdef BFM_STATS
	root->leaf_nodes[kind]++;
#endif

	node = bfm_alloc_node(root, false, kind, size);

	memset(&node->b, 0, sizeof(node->b));
	node->b.kind = kind;

	return node;
}


static bfm_tree_node_inner_1 *
bfm_alloc_inner_1(bfm_tree *root)
{
	bfm_tree_node_inner_1 *node =
		(bfm_tree_node_inner_1 *) bfm_alloc_inner(root, BFM_KIND_1, sizeof(*node));

	return node;
}

#define BFM_TREE_NODE_INNER_4_INVALID 0xFF
static bfm_tree_node_inner_4 *
bfm_alloc_inner_4(bfm_tree *root)
{
	bfm_tree_node_inner_4 *node =
		(bfm_tree_node_inner_4 *) bfm_alloc_inner(root, BFM_KIND_4, sizeof(*node));

	return node;
}

#define BFM_TREE_NODE_INNER_16_INVALID 0xFF
static bfm_tree_node_inner_16 *
bfm_alloc_inner_16(bfm_tree *root)
{
	bfm_tree_node_inner_16 *node =
		(bfm_tree_node_inner_16 *) bfm_alloc_inner(root, BFM_KIND_16, sizeof(*node));

	return node;
}

#define BFM_TREE_NODE_INNER_32_INVALID 0xFF
static bfm_tree_node_inner_32 *
bfm_alloc_inner_32(bfm_tree *root)
{
	bfm_tree_node_inner_32 *node =
		(bfm_tree_node_inner_32 *) bfm_alloc_inner(root, BFM_KIND_32, sizeof(*node));

	return node;
}

static bfm_tree_node_inner_128 *
bfm_alloc_inner_128(bfm_tree *root)
{
	bfm_tree_node_inner_128 *node =
		(bfm_tree_node_inner_128 *) bfm_alloc_inner(root, BFM_KIND_128, sizeof(*node));

	memset(&node->offsets, BFM_TREE_NODE_128_INVALID, sizeof(node->offsets));

	return node;
}

static bfm_tree_node_inner_max *
bfm_alloc_inner_max(bfm_tree *root)
{
	bfm_tree_node_inner_max *node =
		(bfm_tree_node_inner_max *) bfm_alloc_inner(root, BFM_KIND_MAX, sizeof(*node));

	memset(&node->slots, 0, sizeof(node->slots));

	return node;
}

static bfm_tree_node_leaf_1 *
bfm_alloc_leaf_1(bfm_tree *root)
{
	bfm_tree_node_leaf_1 *node =
		(bfm_tree_node_leaf_1 *) bfm_alloc_leaf(root, BFM_KIND_1, sizeof(*node));

	return node;
}

static bfm_tree_node_leaf_4 *
bfm_alloc_leaf_4(bfm_tree *root)
{
	bfm_tree_node_leaf_4 *node =
		(bfm_tree_node_leaf_4 *) bfm_alloc_leaf(root, BFM_KIND_4, sizeof(*node));

	return node;
}

static bfm_tree_node_leaf_16 *
bfm_alloc_leaf_16(bfm_tree *root)
{
	bfm_tree_node_leaf_16 *node =
		(bfm_tree_node_leaf_16 *) bfm_alloc_leaf(root, BFM_KIND_16, sizeof(*node));

	return node;
}

static bfm_tree_node_leaf_32 *
bfm_alloc_leaf_32(bfm_tree *root)
{
	bfm_tree_node_leaf_32 *node =
		(bfm_tree_node_leaf_32 *) bfm_alloc_leaf(root, BFM_KIND_32, sizeof(*node));

	return node;
}

static bfm_tree_node_leaf_128 *
bfm_alloc_leaf_128(bfm_tree *root)
{
	bfm_tree_node_leaf_128 *node =
		(bfm_tree_node_leaf_128 *) bfm_alloc_leaf(root, BFM_KIND_128, sizeof(*node));

	memset(node->offsets, BFM_TREE_NODE_128_INVALID, sizeof(node->offsets));

	return node;
}

static bfm_tree_node_leaf_max *
bfm_alloc_leaf_max(bfm_tree *root)
{
	bfm_tree_node_leaf_max *node =
		(bfm_tree_node_leaf_max *) bfm_alloc_leaf(root, BFM_KIND_MAX, sizeof(*node));

	memset(node->set, 0, sizeof(node->set));

	return node;
}

static void
bfm_free_internal(bfm_tree *root, void *p)
{
#if defined(BFM_USE_OS)
	free(p);
#else
	pfree(p);
#endif
}

static void
bfm_free_inner(bfm_tree *root, bfm_tree_node_inner *node)
{
	Assert(node->b.node_shift != 0);

#ifdef BFM_STATS
	root->inner_nodes[node->b.kind]--;
#endif

	bfm_free_internal(root, node);
}

static void
bfm_free_leaf(bfm_tree *root, bfm_tree_node_leaf *node)
{
	Assert(node->b.node_shift == 0);

#ifdef BFM_STATS
	root->leaf_nodes[node->b.kind]--;
#endif

	bfm_free_internal(root, node);
}

#define BFM_LEAF_MAX_SET_OFFSET(i) (i / (sizeof(uint8) * BITS_PER_BYTE))
#define BFM_LEAF_MAX_SET_BIT(i) (UINT64_C(1) << (i & ((sizeof(uint8) * BITS_PER_BYTE)-1)))

static inline bool
bfm_leaf_max_isset(bfm_tree_node_leaf_max *node_max, uint32 i)
{
	return node_max->set[BFM_LEAF_MAX_SET_OFFSET(i)] & BFM_LEAF_MAX_SET_BIT(i);
}

static inline void
bfm_leaf_max_set(bfm_tree_node_leaf_max *node_max, uint32 i)
{
	node_max->set[BFM_LEAF_MAX_SET_OFFSET(i)] |= BFM_LEAF_MAX_SET_BIT(i);
}

static inline void
bfm_leaf_max_unset(bfm_tree_node_leaf_max *node_max, uint32 i)
{
	node_max->set[BFM_LEAF_MAX_SET_OFFSET(i)] &= ~BFM_LEAF_MAX_SET_BIT(i);
}

static uint64
bfm_maxval_shift(uint32 shift)
{
	uint32 maxshift = (sizeof(bfm_key_type) * BITS_PER_BYTE) / BFM_FANOUT  * BFM_FANOUT;

	Assert(shift <= maxshift);

	if (shift == maxshift)
		return UINT64_MAX;

	return (UINT64_C(1) << (shift + BFM_FANOUT)) - 1;
}

static inline int
search_chunk_array_4_eq(uint8 *chunks, uint8 match, uint8 count)
{
	int index = -1;

	for (int i = 0; i < count; i++)
	{
		if (chunks[i] == match)
		{
			index = i;
			break;
		}
	}

	return index;
}

static inline int
search_chunk_array_4_le(uint8 *chunks, uint8 match, uint8 count)
{
	int index;

	for (index = 0; index < count; index++)
		if (chunks[index] >= match)
			break;

	return index;
}


#if defined(__SSE2__)
#include <emmintrin.h> // x86 SSE intrinsics
#endif

static inline int
search_chunk_array_16_eq(uint8 *chunks, uint8 match, uint8 count)
{
#if !defined(__SSE2__) || defined(USE_ASSERT_CHECKING)
	int index = -1;
#endif

#ifdef __SSE2__
	int index_sse;
	__m128i spread_chunk = _mm_set1_epi8(match);
	__m128i haystack = _mm_loadu_si128((__m128i_u*) chunks);
	__m128i cmp=_mm_cmpeq_epi8(spread_chunk, haystack);
	uint32_t bitfield=_mm_movemask_epi8(cmp);

	bitfield &= ((1<<count)-1);

	if (bitfield)
		index_sse = __builtin_ctz(bitfield);
	else
		index_sse = -1;

#endif

#if !defined(__SSE2__) || defined(USE_ASSERT_CHECKING)
	for (int i = 0; i < count; i++)
	{
		if (chunks[i] == match)
		{
			index = i;
			break;
		}
	}

#if defined(__SSE2__)
	Assert(index_sse == index);
#endif

#endif

#if defined(__SSE2__)
	return index_sse;
#else
	return index;
#endif
}

/*
 * This is a bit more complicated than search_chunk_array_16_eq(), because
 * until recently no unsigned uint8 comparison instruction existed on x86. So
 * we need to play some trickery using _mm_min_epu8() to effectively get
 * <=. There never will be any equal elements in the current uses, but that's
 * what we get here...
 */
static inline int
search_chunk_array_16_le(uint8 *chunks, uint8 match, uint8 count)
{
#if !defined(__SSE2__) || defined(USE_ASSERT_CHECKING)
	int index;
#endif

#ifdef __SSE2__
	int index_sse;
	__m128i spread_chunk = _mm_set1_epi8(match);
	__m128i haystack = _mm_loadu_si128((__m128i_u*) chunks);
	__m128i min = _mm_min_epu8(haystack, spread_chunk);
	__m128i cmp = _mm_cmpeq_epi8(spread_chunk, min);
	uint32_t bitfield=_mm_movemask_epi8(cmp);

	bitfield &= ((1<<count)-1);

	if (bitfield)
		index_sse = __builtin_ctz(bitfield);
	else
		index_sse = count;
#endif

#if !defined(__SSE2__) || defined(USE_ASSERT_CHECKING)
	for (index = 0; index < count; index++)
		if (chunks[index] >= match)
			break;

#if defined(__SSE2__)
	Assert(index_sse == index);
#endif

#endif

#if defined(__SSE2__)
	return index_sse;
#else
	return index;
#endif
}

#if defined(__AVX2__)
#include <immintrin.h> // x86 SSE intrinsics
#endif

static inline int
search_chunk_array_32_eq(uint8 *chunks, uint8 match, uint8 count)
{
#if !defined(__AVX2__) || defined(USE_ASSERT_CHECKING)
	int index = -1;
#endif

#ifdef __AVX2__
	int index_sse;
	__m256i spread_chunk = _mm256_set1_epi8(match);
	__m256i haystack = _mm256_loadu_si256((__m256i_u*) chunks);
	__m256i cmp= _mm256_cmpeq_epi8(spread_chunk, haystack);
	uint32_t bitfield = _mm256_movemask_epi8(cmp);

	bitfield &= ((UINT64_C(1)<<count)-1);

	if (bitfield)
		index_sse = __builtin_ctz(bitfield);
	else
		index_sse = -1;

#endif

#if !defined(__AVX2__) || defined(USE_ASSERT_CHECKING)
	for (int i = 0; i < count; i++)
	{
		if (chunks[i] == match)
		{
			index = i;
			break;
		}
	}

#if defined(__AVX2__)
	Assert(index_sse == index);
#endif

#endif

#if defined(__AVX2__)
	return index_sse;
#else
	return index;
#endif
}

/*
 * This is a bit more complicated than search_chunk_array_16_eq(), because
 * until recently no unsigned uint8 comparison instruction existed on x86. So
 * we need to play some trickery using _mm_min_epu8() to effectively get
 * <=. There never will be any equal elements in the current uses, but that's
 * what we get here...
 */
static inline int
search_chunk_array_32_le(uint8 *chunks, uint8 match, uint8 count)
{
#if !defined(__AVX2__) || defined(USE_ASSERT_CHECKING)
	int index;
#endif

#ifdef __AVX2__
	int index_sse;
	__m256i spread_chunk = _mm256_set1_epi8(match);
	__m256i haystack = _mm256_loadu_si256((__m256i_u*) chunks);
	__m256i min = _mm256_min_epu8(haystack, spread_chunk);
	__m256i cmp=_mm256_cmpeq_epi8(spread_chunk, min);
	uint32_t bitfield=_mm256_movemask_epi8(cmp);

	bitfield &= ((1<<count)-1);

	if (bitfield)
		index_sse = __builtin_ctz(bitfield);
	else
		index_sse = count;
#endif

#if !defined(__AVX2__) || defined(USE_ASSERT_CHECKING)
	for (index = 0; index < count; index++)
		if (chunks[index] >= match)
			break;

#if defined(__AVX2__)
	Assert(index_sse == index);
#endif

#endif

#if defined(__AVX2__)
	return index_sse;
#else
	return index;
#endif
}

static inline void
chunk_slot_array_grow(uint8 *source_chunks, bfm_tree_node **source_slots,
					  uint8 *target_chunks, bfm_tree_node **target_slots,
					  bfm_tree_node_inner *oldnode, bfm_tree_node_inner *newnode)
{
	memcpy(target_chunks, source_chunks, sizeof(source_chunks[0]) * oldnode->b.count);
	memcpy(target_slots, source_slots, sizeof(source_slots[0]) * oldnode->b.count);

	for (int i = 0; i < oldnode->b.count; i++)
	{
		Assert(source_slots[i]->parent == oldnode);
		source_slots[i]->parent = newnode;
	}
}

/*
 * FIXME: Find a way to deduplicate with bfm_find_one_level_inner()
 */
pg_attribute_always_inline static bfm_tree_node *
bfm_find_one_level_inner(bfm_tree_node_inner * pg_restrict node, uint8 chunk)
{
	bfm_tree_node *slot = NULL;

	Assert(node->b.node_shift != 0); /* is inner node */

	/* tell the compiler it doesn't need a bounds check */
	if ((bfm_tree_node_kind) node->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_inner_1 *node_1 =
					(bfm_tree_node_inner_1 *) node;

				Assert(node_1->b.b.count <= 1);
				if (node_1->chunk == chunk)
					slot = node_1->slot;
				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_inner_4 *node_4 =
					(bfm_tree_node_inner_4 *) node;
				int index;

				Assert(node_4->b.b.count <= 4);
				index = search_chunk_array_4_eq(node_4->chunks, chunk, node_4->b.b.count);

				if (index != -1)
					slot = node_4->slots[index];

				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_inner_16 *node_16 =
					(bfm_tree_node_inner_16 *) node;
				int index;

				Assert(node_16->b.b.count <= 16);

				index = search_chunk_array_16_eq(node_16->chunks, chunk, node_16->b.b.count);
				if (index != -1)
					slot = node_16->slots[index];

				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_inner_32 *node_32 =
					(bfm_tree_node_inner_32 *) node;
				int index;

				Assert(node_32->b.b.count <= 32);

				index = search_chunk_array_32_eq(node_32->chunks, chunk, node_32->b.b.count);
				if (index != -1)
					slot = node_32->slots[index];

				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_inner_128 *node_128 =
					(bfm_tree_node_inner_128 *) node;

				Assert(node_128->b.b.count <= 128);

				if (node_128->offsets[chunk] != BFM_TREE_NODE_128_INVALID)
				{
					slot = node_128->slots[node_128->offsets[chunk]];
				}
				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_inner_max *node_max =
					(bfm_tree_node_inner_max *) node;

				Assert(node_max->b.b.count <= BFM_MAX_CLASS);
				slot = node_max->slots[chunk];

				break;
			}
	}

	return slot;
}

/*
 * FIXME: Find a way to deduplicate with bfm_find_one_level_inner()
 */
pg_attribute_always_inline static bool
bfm_find_one_level_leaf(bfm_tree_node_leaf * pg_restrict node, uint8 chunk, bfm_value_type * pg_restrict valp)
{
	bool found = false;

	Assert(node->b.node_shift == 0); /* is leaf node */

	/* tell the compiler it doesn't need a bounds check */
	if ((bfm_tree_node_kind) node->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_leaf_1 *node_1 =
					(bfm_tree_node_leaf_1 *) node;

				Assert(node_1->b.b.count <= 1);
				if (node_1->b.b.count == 1 &&
					node_1->chunk == chunk)
				{
					*valp = node_1->value;
					found = true;
					break;
				}
				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_leaf_4 *node_4 =
					(bfm_tree_node_leaf_4 *) node;
				int index;

				Assert(node_4->b.b.count <= 4);
				index = search_chunk_array_4_eq(node_4->chunks, chunk, node_4->b.b.count);

				if (index != -1)
				{
					*valp = node_4->values[index];
					found = true;
				}
				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_leaf_16 *node_16 =
					(bfm_tree_node_leaf_16 *) node;
				int index;

				Assert(node_16->b.b.count <= 16);

				index = search_chunk_array_16_eq(node_16->chunks, chunk, node_16->b.b.count);
				if (index != -1)
				{
					*valp = node_16->values[index];
					found = true;
					break;
				}
				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_leaf_32 *node_32 =
					(bfm_tree_node_leaf_32 *) node;
				int index;

				Assert(node_32->b.b.count <= 32);

				index = search_chunk_array_32_eq(node_32->chunks, chunk, node_32->b.b.count);
				if (index != -1)
				{
					*valp = node_32->values[index];
					found = true;
					break;
				}
				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_leaf_128 *node_128 =
					(bfm_tree_node_leaf_128 *) node;

				Assert(node_128->b.b.count <= 128);

				if (node_128->offsets[chunk] != BFM_TREE_NODE_128_INVALID)
				{
					*valp = node_128->values[node_128->offsets[chunk]];
					found = true;
				}
				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_leaf_max *node_max =
					(bfm_tree_node_leaf_max *) node;

				Assert(node_max->b.b.count <= BFM_MAX_CLASS);

				if (bfm_leaf_max_isset(node_max, chunk))
				{
					*valp = node_max->values[chunk];
					found = true;
				}
				break;
			}
	}

	return found;
}

pg_attribute_always_inline static bool
bfm_walk(bfm_tree *root, bfm_tree_node **nodep, bfm_value_type *valp, uint64_t key)
{
	bfm_tree_node *rnode;
	bfm_tree_node *cur;
	uint8 chunk;
	uint32 shift;

	rnode = root->rnode;

	/* can't be contained in the tree */
	if (!rnode || key > root->maxval)
	{
		*nodep = NULL;
		return false;
	}

	shift = rnode->node_shift;
	chunk = (key >> shift) & BFM_MASK;
	cur = rnode;

	while (shift > 0)
	{
		bfm_tree_node_inner *cur_inner;
		bfm_tree_node *slot;

		Assert(cur->node_shift > 0); /* leaf nodes look different */
		Assert(cur->node_shift == shift);

		cur_inner = (bfm_tree_node_inner *) cur;

		slot = bfm_find_one_level_inner(cur_inner, chunk);

		if (slot == NULL)
		{
			*nodep = cur;
			return false;
		}

		Assert(&slot->parent->b == cur);
		Assert(slot->node_chunk == chunk);

		cur = slot;
		shift -= BFM_FANOUT;
		chunk = (key >> shift) & BFM_MASK;
	}

	Assert(cur->node_shift == shift && shift == 0);

	*nodep = cur;

	return bfm_find_one_level_leaf((bfm_tree_node_leaf*) cur, chunk, valp);
}

/*
 * Redirect parent pointers to oldnode by newnode, for the key chunk
 * chunk. Used when growing or shrinking nodes.
 */
static void
bfm_redirect(bfm_tree *root, bfm_tree_node *oldnode, bfm_tree_node *newnode, uint8 chunk)
{
	bfm_tree_node_inner *parent = oldnode->parent;

	if (parent == NULL)
	{
		Assert(root->rnode == oldnode);
		root->rnode = newnode;
		return;
	}

	/* if there is a parent, it needs to be an inner node */
	Assert(parent->b.node_shift != 0);

	if ((bfm_tree_node_kind) parent->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) parent->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_inner_1 *parent_1 =
					(bfm_tree_node_inner_1 *) parent;

				Assert(parent_1->slot == oldnode);
				Assert(parent_1->chunk == chunk);

				parent_1->slot = newnode;
				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_inner_4 *parent_4 =
					(bfm_tree_node_inner_4 *) parent;
				int index;

				Assert(parent_4->b.b.count <= 4);
				index = search_chunk_array_4_eq(parent_4->chunks, chunk, parent_4->b.b.count);
				Assert(index != -1);

				Assert(parent_4->slots[index] == oldnode);
				parent_4->slots[index] = newnode;

				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_inner_16 *parent_16 =
					(bfm_tree_node_inner_16 *) parent;
				int index;

				index = search_chunk_array_16_eq(parent_16->chunks, chunk, parent_16->b.b.count);
				Assert(index != -1);

				Assert(parent_16->slots[index] == oldnode);
				parent_16->slots[index] = newnode;
				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_inner_32 *parent_32 =
					(bfm_tree_node_inner_32 *) parent;
				int index;

				index = search_chunk_array_32_eq(parent_32->chunks, chunk, parent_32->b.b.count);
				Assert(index != -1);

				Assert(parent_32->slots[index] == oldnode);
				parent_32->slots[index] = newnode;
				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_inner_128 *parent_128 =
					(bfm_tree_node_inner_128 *) parent;
				uint8 offset;

				offset = parent_128->offsets[chunk];
				Assert(offset != BFM_TREE_NODE_128_INVALID);
				Assert(parent_128->slots[offset] == oldnode);
				parent_128->slots[offset] = newnode;
				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_inner_max *parent_max =
					(bfm_tree_node_inner_max *) parent;

				Assert(parent_max->slots[chunk] == oldnode);
				parent_max->slots[chunk] = newnode;

				break;
			}
	}
}

static void
bfm_node_copy_common(bfm_tree *root, bfm_tree_node *oldnode, bfm_tree_node *newnode)
{
	newnode->node_shift = oldnode->node_shift;
	newnode->node_chunk = oldnode->node_chunk;
	newnode->count = oldnode->count;
	newnode->parent = oldnode->parent;
}

/*
 * Insert child into node.
 *
 * NB: `node` cannot be used after this call anymore, it changes if the node
 * needs to be grown to fit the insertion.
 *
 * FIXME: Find a way to deduplicate with bfm_set_leaf()
 */
static void
bfm_insert_inner(bfm_tree *root, bfm_tree_node_inner *node, bfm_tree_node *child, int child_chunk)
{
	Assert(node->b.node_shift != 0); /* is inner node */

	child->node_chunk = child_chunk;

	/* tell the compiler it doesn't need a bounds check */
	if ((bfm_tree_node_kind) node->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_inner_1 *node_1 =
					(bfm_tree_node_inner_1 *) node;

				Assert(node_1->b.b.count <= 1);

				if (unlikely(node_1->b.b.count == 1))
				{
					/* grow node from 1 -> 4 */
					bfm_tree_node_inner_4 *newnode_4;

					newnode_4 = bfm_alloc_inner_4(root);
					bfm_node_copy_common(root, &node->b, &newnode_4->b.b);

					Assert(node_1->slot->parent != NULL);
					Assert(node_1->slot->parent == node);
					newnode_4->chunks[0] = node_1->chunk;
					newnode_4->slots[0] = node_1->slot;
					node_1->slot->parent = &newnode_4->b;

					bfm_redirect(root, &node->b, &newnode_4->b.b, newnode_4->b.b.node_chunk);
					bfm_free_inner(root, node);
					node = &newnode_4->b;
				}
				else
				{
					child->parent = node;
					node_1->chunk = child_chunk;
					node_1->slot = child;
					break;
				}
			}
			/* fallthrough */

		case BFM_KIND_4:
			{
				bfm_tree_node_inner_4 *node_4 =
					(bfm_tree_node_inner_4 *) node;

				Assert(node_4->b.b.count <= 4);
				if (unlikely(node_4->b.b.count == 4))
				{
					/* grow node from 4 -> 16 */
					bfm_tree_node_inner_16 *newnode_16;

					newnode_16 = bfm_alloc_inner_16(root);
					bfm_node_copy_common(root, &node->b, &newnode_16->b.b);

					chunk_slot_array_grow(node_4->chunks, node_4->slots,
										  newnode_16->chunks, newnode_16->slots,
										  &node_4->b, &newnode_16->b);

					bfm_redirect(root, &node->b, &newnode_16->b.b, newnode_16->b.b.node_chunk);
					bfm_free_inner(root, node);
					node = &newnode_16->b;
				}
				else
				{
					int insertpos;

					for (insertpos = 0; insertpos < node_4->b.b.count; insertpos++)
						if (node_4->chunks[insertpos] >= child_chunk)
							break;

					child->parent = node;

					memmove(&node_4->slots[insertpos + 1],
							&node_4->slots[insertpos],
							(node_4->b.b.count - insertpos) * sizeof(node_4->slots[0]));
					memmove(&node_4->chunks[insertpos + 1],
							&node_4->chunks[insertpos],
							(node_4->b.b.count - insertpos) * sizeof(node_4->chunks[0]));

					node_4->chunks[insertpos] = child_chunk;
					node_4->slots[insertpos] = child;
					break;
				}
			}
			/* fallthrough */

		case BFM_KIND_16:
			{
				bfm_tree_node_inner_16 *node_16 =
					(bfm_tree_node_inner_16 *) node;

				Assert(node_16->b.b.count <= 16);
				if (unlikely(node_16->b.b.count == 16))
				{
					/* grow node from 16 -> 32 */
					bfm_tree_node_inner_32 *newnode_32;

					newnode_32 = bfm_alloc_inner_32(root);
					bfm_node_copy_common(root, &node->b, &newnode_32->b.b);

					chunk_slot_array_grow(node_16->chunks, node_16->slots,
										  newnode_32->chunks, newnode_32->slots,
										  &node_16->b, &newnode_32->b);

					bfm_redirect(root, &node->b, &newnode_32->b.b, newnode_32->b.b.node_chunk);
					bfm_free_inner(root, node);
					node = &newnode_32->b;
				}
				else
				{
					int insertpos;

					insertpos = search_chunk_array_16_le(node_16->chunks, child_chunk, node_16->b.b.count);

					child->parent = node;

					memmove(&node_16->slots[insertpos + 1],
							&node_16->slots[insertpos],
							(node_16->b.b.count - insertpos) * sizeof(node_16->slots[0]));
					memmove(&node_16->chunks[insertpos + 1],
							&node_16->chunks[insertpos],
							(node_16->b.b.count - insertpos) * sizeof(node_16->chunks[0]));

					node_16->chunks[insertpos] = child_chunk;
					node_16->slots[insertpos] = child;
					break;
				}
			}
			/* fallthrough */

		case BFM_KIND_32:
			{
				bfm_tree_node_inner_32 *node_32 =
					(bfm_tree_node_inner_32 *) node;

				Assert(node_32->b.b.count <= 32);
				if (unlikely(node_32->b.b.count == 32))
				{
					/* grow node from 32 -> 128 */
					bfm_tree_node_inner_128 *newnode_128;

					newnode_128 = bfm_alloc_inner_128(root);
					bfm_node_copy_common(root, &node->b, &newnode_128->b.b);

					memcpy(newnode_128->slots, node_32->slots, sizeof(node_32->slots));

					/* change parent pointers of children */
					for (int i = 0; i < 32; i++)
					{
						Assert(node_32->slots[i]->parent == node);
						newnode_128->offsets[node_32->chunks[i]] = i;
						node_32->slots[i]->parent = &newnode_128->b;
					}

					bfm_redirect(root, &node->b, &newnode_128->b.b, newnode_128->b.b.node_chunk);
					bfm_free_inner(root, node);
					node = &newnode_128->b;
				}
				else
				{
					int insertpos;

					insertpos = search_chunk_array_32_le(node_32->chunks, child_chunk, node_32->b.b.count);

					child->parent = node;

					memmove(&node_32->slots[insertpos + 1],
							&node_32->slots[insertpos],
							(node_32->b.b.count - insertpos) * sizeof(node_32->slots[0]));
					memmove(&node_32->chunks[insertpos + 1],
							&node_32->chunks[insertpos],
							(node_32->b.b.count - insertpos) * sizeof(node_32->chunks[0]));

					node_32->chunks[insertpos] = child_chunk;
					node_32->slots[insertpos] = child;
					break;
				}
			}
			/* fallthrough */

		case BFM_KIND_128:
			{
				bfm_tree_node_inner_128 *node_128 =
					(bfm_tree_node_inner_128 *) node;
				uint8 offset;

				Assert(node_128->b.b.count <= 128);
				if (unlikely(node_128->b.b.count == 128))
				{
					/* grow node from 128 -> max */
					bfm_tree_node_inner_max *newnode_max;

					newnode_max = bfm_alloc_inner_max(root);
					bfm_node_copy_common(root, &node->b, &newnode_max->b.b);

					for (int i = 0; i < BFM_MAX_CLASS; i++)
					{
						uint8 offset = node_128->offsets[i];

						if (offset == BFM_TREE_NODE_128_INVALID)
							continue;

						Assert(node_128->slots[offset] != NULL);
						Assert(node_128->slots[offset]->parent == node);

						node_128->slots[offset]->parent = &newnode_max->b;

						newnode_max->slots[i] = node_128->slots[offset];
					}

					bfm_redirect(root, &node->b, &newnode_max->b.b, newnode_max->b.b.node_chunk);
					bfm_free_inner(root, node);
					node = &newnode_max->b;
				}
				else
				{
					child->parent = node;
					offset = node_128->b.b.count;
					/* FIXME: this may overwrite entry if there had been deletions */
					node_128->offsets[child_chunk] = offset;
					node_128->slots[offset] = child;
					break;
				}
			}
			/* fallthrough */

		case BFM_KIND_MAX:
			{
				bfm_tree_node_inner_max *node_max =
					(bfm_tree_node_inner_max *) node;

				Assert(node_max->b.b.count <= (BFM_MAX_CLASS - 1));
				Assert(node_max->slots[child_chunk] == NULL);

				child->parent = node;
				node_max->slots[child_chunk] = child;

				break;
			}
	}

	node->b.count++;
}

static bool pg_noinline
bfm_grow_leaf_1(bfm_tree *root, bfm_tree_node_leaf_1 *node_1,
				int child_chunk, bfm_value_type val)
{
	/* grow node from 1 -> 4 */
	bfm_tree_node_leaf_4 *newnode_4;

	Assert(node_1->b.b.count == 1);

	newnode_4 = bfm_alloc_leaf_4(root);
	bfm_node_copy_common(root, &node_1->b.b, &newnode_4->b.b);

	/* copy old & insert new value in the right order */
	if (child_chunk < node_1->chunk)
	{
		newnode_4->chunks[0] = child_chunk;
		newnode_4->values[0] = val;
		newnode_4->chunks[1] = node_1->chunk;
		newnode_4->values[1] = node_1->value;
	}
	else
	{
		newnode_4->chunks[0] = node_1->chunk;
		newnode_4->values[0] = node_1->value;
		newnode_4->chunks[1] = child_chunk;
		newnode_4->values[1] = val;
	}

	newnode_4->b.b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_redirect(root, &node_1->b.b, &newnode_4->b.b, newnode_4->b.b.node_chunk);
	bfm_free_leaf(root, &node_1->b);

	return false;
}

static bool pg_noinline
bfm_grow_leaf_4(bfm_tree *root, bfm_tree_node_leaf_4 *node_4,
				int child_chunk, bfm_value_type val)
{
	/* grow node from 4 -> 16 */
	bfm_tree_node_leaf_16 *newnode_16;
	int insertpos;

	Assert(node_4->b.b.count == 4);

	newnode_16 = bfm_alloc_leaf_16(root);
	bfm_node_copy_common(root, &node_4->b.b, &newnode_16->b.b);

	insertpos = search_chunk_array_4_le(node_4->chunks, child_chunk, node_4->b.b.count);

	/* first copy old elements ordering before */
	memcpy(&newnode_16->chunks[0],
		   &node_4->chunks[0],
		   sizeof(node_4->chunks[0]) * insertpos);
	memcpy(&newnode_16->values[0],
		   &node_4->values[0],
		   sizeof(node_4->values[0]) * insertpos);

	/* then the new element */
	newnode_16->chunks[insertpos] = child_chunk;
	newnode_16->values[insertpos] = val;

	/* and lastly the old elements after */
	memcpy(&newnode_16->chunks[insertpos + 1],
		   &node_4->chunks[insertpos],
		   (node_4->b.b.count-insertpos) * sizeof(node_4->chunks[0]));
	memcpy(&newnode_16->values[insertpos + 1],
		   &node_4->values[insertpos],
		   (node_4->b.b.count-insertpos) * sizeof(node_4->values[0]));

	newnode_16->b.b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_redirect(root, &node_4->b.b, &newnode_16->b.b, newnode_16->b.b.node_chunk);
	bfm_free_leaf(root, &node_4->b);

	return false;
}

static bool pg_noinline
bfm_grow_leaf_16(bfm_tree *root, bfm_tree_node_leaf_16 *node_16,
				int child_chunk, bfm_value_type val)
{
	/* grow node from 16 -> 32 */
	bfm_tree_node_leaf_32 *newnode_32;
	int insertpos;

	Assert(node_16->b.b.count == 16);

	newnode_32 = bfm_alloc_leaf_32(root);
	bfm_node_copy_common(root, &node_16->b.b, &newnode_32->b.b);

	insertpos = search_chunk_array_16_le(node_16->chunks, child_chunk, node_16->b.b.count);

	/* first copy old elements ordering before */
	memcpy(&newnode_32->chunks[0],
		   &node_16->chunks[0],
		   sizeof(node_16->chunks[0]) * insertpos);
	memcpy(&newnode_32->values[0],
		   &node_16->values[0],
		   sizeof(node_16->values[0]) * insertpos);

	/* then the new element */
	newnode_32->chunks[insertpos] = child_chunk;
	newnode_32->values[insertpos] = val;

	/* and lastly the old elements after */
	memcpy(&newnode_32->chunks[insertpos + 1],
		   &node_16->chunks[insertpos],
		   (node_16->b.b.count-insertpos) * sizeof(node_16->chunks[0]));
	memcpy(&newnode_32->values[insertpos + 1],
		   &node_16->values[insertpos],
		   (node_16->b.b.count-insertpos) * sizeof(node_16->values[0]));

	newnode_32->b.b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_redirect(root, &node_16->b.b, &newnode_32->b.b, newnode_32->b.b.node_chunk);
	bfm_free_leaf(root, &node_16->b);

	return false;
}

static bool pg_noinline
bfm_grow_leaf_32(bfm_tree *root, bfm_tree_node_leaf_32 *node_32,
				 int child_chunk, bfm_value_type val)
{
	/* grow node from 32 -> 128 */
	bfm_tree_node_leaf_128 *newnode_128;
	uint8 offset;

	newnode_128 = bfm_alloc_leaf_128(root);
	bfm_node_copy_common(root, &node_32->b.b, &newnode_128->b.b);

	memcpy(newnode_128->values, node_32->values, sizeof(node_32->values));

	for (int i = 0; i < 32; i++)
		newnode_128->offsets[node_32->chunks[i]] = i;

	offset = newnode_128->b.b.count;
	newnode_128->offsets[child_chunk] = offset;
	newnode_128->values[offset] = val;

	newnode_128->b.b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_redirect(root, &node_32->b.b, &newnode_128->b.b, newnode_128->b.b.node_chunk);
	bfm_free_leaf(root, &node_32->b);

	return false;
}

static bool pg_noinline
bfm_grow_leaf_128(bfm_tree *root, bfm_tree_node_leaf_128 *node_128,
				  int child_chunk, bfm_value_type val)
{
	/* grow node from 128 -> max */
	bfm_tree_node_leaf_max *newnode_max;
	int i;

	newnode_max = bfm_alloc_leaf_max(root);
	bfm_node_copy_common(root, &node_128->b.b, &newnode_max->b.b);

	/*
	 * The bitmask manipulation is a surprisingly large portion of the
	 * overhead in the naive implementation. Unrolling the bit manipulation
	 * removes a lot of that overhead.
	 */
	i = 0;
	for (int byte = 0; byte < BFM_MAX_CLASS / BITS_PER_BYTE; byte++)
	{
		uint8 bitmap = 0;

		for (int bit = 0; bit < BITS_PER_BYTE; bit++)
		{
			uint8 offset = node_128->offsets[i];

			if (offset != BFM_TREE_NODE_128_INVALID)
			{
				bitmap |= 1 << bit;
				newnode_max->values[i] = node_128->values[offset];
			}

			i++;
		}

		newnode_max->set[byte] = bitmap;
	}

	bfm_leaf_max_set(newnode_max, child_chunk);
	newnode_max->values[child_chunk] = val;
	newnode_max->b.b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_redirect(root, &node_128->b.b, &newnode_max->b.b, newnode_max->b.b.node_chunk);
	bfm_free_leaf(root, &node_128->b);

	return false;
}

/*
 * Set key to val. Return false if entry doesn't yet exist, true if it did.
 *
 * See comments to bfm_insert_inner().
 */
static bool pg_noinline
bfm_set_leaf(bfm_tree *root, bfm_key_type key, bfm_value_type val,
			 bfm_tree_node_leaf *node, int child_chunk)
{
	Assert(node->b.node_shift == 0); /* is leaf node */

	/* tell the compiler it doesn't need a bounds check */
	if ((bfm_tree_node_kind) node->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_leaf_1 *node_1 =
					(bfm_tree_node_leaf_1 *) node;

				Assert(node_1->b.b.count <= 1);

				if (node_1->b.b.count == 1 &&
					node_1->chunk == child_chunk)
				{
					node_1->value = val;
					return true;
				}
				else if (likely(node_1->b.b.count < 1))
				{
					node_1->chunk = child_chunk;
					node_1->value = val;
				}
				else
					return bfm_grow_leaf_1(root, node_1, child_chunk, val);

				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_leaf_4 *node_4 =
					(bfm_tree_node_leaf_4 *) node;
				int index;

				Assert(node_4->b.b.count <= 4);

				index = search_chunk_array_4_eq(node_4->chunks, child_chunk, node_4->b.b.count);
				if (index != -1)
				{
					node_4->values[index] = val;
					return true;
				}

				if (likely(node_4->b.b.count < 4))
				{
					int insertpos;

					insertpos = search_chunk_array_4_le(node_4->chunks, child_chunk, node_4->b.b.count);

					for (int i = node_4->b.b.count - 1; i >= insertpos; i--)
					{
						/* workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101481 */
#ifdef __GNUC__
						__asm__("");
#endif
						node_4->values[i + 1] = node_4->values[i];
						node_4->chunks[i + 1] = node_4->chunks[i];
					}

					node_4->chunks[insertpos] = child_chunk;
					node_4->values[insertpos] = val;
				}
				else
					return bfm_grow_leaf_4(root, node_4, child_chunk, val);

				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_leaf_16 *node_16 =
					(bfm_tree_node_leaf_16 *) node;
				int index;

				Assert(node_16->b.b.count <= 16);

				index = search_chunk_array_16_eq(node_16->chunks, child_chunk, node_16->b.b.count);
				if (index != -1)
				{
					node_16->values[index] = val;
					return true;
				}

				if (likely(node_16->b.b.count < 16))
				{
					int insertpos;

					insertpos = search_chunk_array_16_le(node_16->chunks, child_chunk, node_16->b.b.count);

					if (node_16->b.b.count > 16 || insertpos > 15)
						pg_unreachable();

					for (int i = node_16->b.b.count - 1; i >= insertpos; i--)
					{
						/* workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101481 */
#ifdef __GNUC__
						__asm__("");
#endif
						node_16->values[i + 1] = node_16->values[i];
						node_16->chunks[i + 1] = node_16->chunks[i];
					}
					node_16->chunks[insertpos] = child_chunk;
					node_16->values[insertpos] = val;
				}
				else
					return bfm_grow_leaf_16(root, node_16, child_chunk, val);

				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_leaf_32 *node_32 =
					(bfm_tree_node_leaf_32 *) node;
				int index;

				Assert(node_32->b.b.count <= 32);

				index = search_chunk_array_32_eq(node_32->chunks, child_chunk, node_32->b.b.count);
				if (index != -1)
				{
					node_32->values[index] = val;
					return true;
				}

				if (likely(node_32->b.b.count < 32))
				{
					int insertpos;

					insertpos = search_chunk_array_32_le(node_32->chunks, child_chunk, node_32->b.b.count);

					if (node_32->b.b.count > 32 || insertpos > 31)
						pg_unreachable();

					for (int i = node_32->b.b.count - 1; i >= insertpos; i--)
					{
						/* workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101481 */
#ifdef __GNUC__
						__asm__("");
#endif
						node_32->values[i + 1] = node_32->values[i];
						node_32->chunks[i + 1] = node_32->chunks[i];
					}
					node_32->chunks[insertpos] = child_chunk;
					node_32->values[insertpos] = val;
				}
				else
					return bfm_grow_leaf_32(root, node_32, child_chunk, val);

				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_leaf_128 *node_128 =
					(bfm_tree_node_leaf_128 *) node;
				uint8 offset;

				Assert(node_128->b.b.count <= 128);

				if (node_128->offsets[child_chunk] != BFM_TREE_NODE_128_INVALID)
				{
					offset = node_128->offsets[child_chunk];
					node_128->values[offset] = val;

					return true;
				}
				else if (likely(node_128->b.b.count < 128))
				{
					offset = node_128->b.b.count;
					node_128->offsets[child_chunk] = offset;
					node_128->values[offset] = val;
				}
				else
					return bfm_grow_leaf_128(root, node_128, child_chunk, val);

				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_leaf_max *node_max =
					(bfm_tree_node_leaf_max *) node;

				Assert(node_max->b.b.count <= (BFM_MAX_CLASS - 1));

				if (bfm_leaf_max_isset(node_max, child_chunk))
				{
					node_max->values[child_chunk] = val;
					return true;
				}

				bfm_leaf_max_set(node_max, child_chunk);
				node_max->values[child_chunk] = val;

				break;
			}
	}

	node->b.count++;
#ifdef BFM_STATS
	root->entries++;
#endif

	return false;
}

static bool pg_noinline
bfm_set_extend(bfm_tree *root, bfm_key_type key, bfm_value_type val,
			   bfm_tree_node_inner *cur_inner,
			   uint32 shift, uint8 chunk)
{
	bfm_tree_node_leaf_1 *new_leaf_1;

	while (shift > BFM_FANOUT)
	{
		bfm_tree_node_inner_1 *new_inner_1;

		Assert(shift == cur_inner->b.node_shift);

		new_inner_1 = bfm_alloc_inner_1(root);
		new_inner_1->b.b.node_shift = shift - BFM_FANOUT;

		bfm_insert_inner(root, cur_inner, &new_inner_1->b.b, chunk);

		shift -= BFM_FANOUT;
		chunk = (key >> shift) & BFM_MASK;
		cur_inner = &new_inner_1->b;
	}

	Assert(shift == BFM_FANOUT && cur_inner->b.node_shift == BFM_FANOUT);

	new_leaf_1 = bfm_alloc_leaf_1(root);
	new_leaf_1->b.b.count = 1;
	new_leaf_1->b.b.node_shift = 0;

	new_leaf_1->chunk = key & BFM_MASK;
	new_leaf_1->value = val;

#ifdef BFM_STATS
	root->entries++;
#endif

	bfm_insert_inner(root, cur_inner, &new_leaf_1->b.b, chunk);

	return false;
}

static bool pg_noinline
bfm_set_empty(bfm_tree *root, bfm_key_type key, bfm_value_type val)
{
	uint32 shift;

	Assert(root->rnode == NULL);

	if (key == 0)
		shift = 0;
	else
		shift = (pg_leftmost_one_pos64(key)/BFM_FANOUT)*BFM_FANOUT;

	if (shift == 0)
	{
		bfm_tree_node_leaf_1 *nroot = bfm_alloc_leaf_1(root);

		Assert((key & BFM_MASK) == key);

		nroot->b.b.node_shift = 0;
		nroot->b.b.node_chunk = 0;
		nroot->b.b.parent = NULL;

		root->maxval = bfm_maxval_shift(0);

		root->rnode = &nroot->b.b;

		return bfm_set_leaf(root, key, val, &nroot->b, key);
	}
	else
	{
		bfm_tree_node_inner_1 *nroot = bfm_alloc_inner_1(root);

		nroot->b.b.node_shift = shift;
		nroot->b.b.node_chunk = 0;
		nroot->b.b.parent = NULL;

		root->maxval = bfm_maxval_shift(shift);
		root->rnode = &nroot->b.b;


		return bfm_set_extend(root, key, val, &nroot->b,
								 shift, (key >> shift) & BFM_MASK);
	}
}

/*
 * Tree doesn't have sufficient height. Put new tree node(s) on top, move
 * the old node below it, and then insert.
 */
static bool pg_noinline
bfm_set_shallow(bfm_tree *root, bfm_key_type key, bfm_value_type val)
{
	uint32 shift;
	bfm_tree_node_inner_1 *nroot = NULL;;

	Assert(root->rnode != NULL);

	if (key == 0)
		shift = 0;
	else
		shift = (pg_leftmost_one_pos64(key)/BFM_FANOUT)*BFM_FANOUT;

	Assert(root->rnode->node_shift < shift);

	while (unlikely(root->rnode->node_shift < shift))
	{
		nroot = bfm_alloc_inner_1(root);

		nroot->slot = root->rnode;
		nroot->chunk = 0;
		nroot->b.b.count = 1;
		nroot->b.b.parent = NULL;
		nroot->b.b.node_shift = root->rnode->node_shift + BFM_FANOUT;

		root->rnode->parent = &nroot->b;
		root->rnode = &nroot->b.b;

		root->maxval = bfm_maxval_shift(nroot->b.b.node_shift);
	}

	Assert(nroot != NULL);

	return bfm_set_extend(root, key, val, &nroot->b,
							 shift, (key >> shift) & BFM_MASK);
}

static void
bfm_delete_inner(bfm_tree * pg_restrict root, bfm_tree_node_inner * pg_restrict node, bfm_tree_node *pg_restrict child, int child_chunk)
{
	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_inner_1 *node_1 =
					(bfm_tree_node_inner_1 *) node;

				Assert(node_1->slot == child);
				Assert(node_1->chunk == child_chunk);

				node_1->chunk = 17;
				node_1->slot = NULL;

				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_inner_4 *node_4 =
					(bfm_tree_node_inner_4 *) node;
				int index;

				index = search_chunk_array_4_eq(node_4->chunks, child_chunk, node_4->b.b.count);
				Assert(index != -1);

				Assert(node_4->slots[index] == child);
				memmove(&node_4->slots[index],
						&node_4->slots[index + 1],
						(node_4->b.b.count-index-1) * sizeof(void*));
				memmove(&node_4->chunks[index],
						&node_4->chunks[index + 1],
						node_4->b.b.count-index-1);

				node_4->chunks[node_4->b.b.count - 1] = BFM_TREE_NODE_INNER_4_INVALID;
				node_4->slots[node_4->b.b.count - 1] = NULL;

				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_inner_16 *node_16 =
					(bfm_tree_node_inner_16 *) node;
				int index;

				index = search_chunk_array_16_eq(node_16->chunks, child_chunk, node_16->b.b.count);
				Assert(index != -1);

				Assert(node_16->slots[index] == child);
				memmove(&node_16->slots[index],
						&node_16->slots[index + 1],
						(node_16->b.b.count - index - 1) * sizeof(node_16->slots[0]));
				memmove(&node_16->chunks[index],
						&node_16->chunks[index + 1],
						(node_16->b.b.count - index - 1) * sizeof(node_16->chunks[0]));

				node_16->chunks[node_16->b.b.count - 1] = BFM_TREE_NODE_INNER_16_INVALID;
				node_16->slots[node_16->b.b.count - 1] = NULL;

				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_inner_32 *node_32 =
					(bfm_tree_node_inner_32 *) node;
				int index;

				index = search_chunk_array_32_eq(node_32->chunks, child_chunk, node_32->b.b.count);
				Assert(index != -1);

				Assert(node_32->slots[index] == child);
				memmove(&node_32->slots[index],
						&node_32->slots[index + 1],
						(node_32->b.b.count - index - 1) * sizeof(node_32->slots[0]));
				memmove(&node_32->chunks[index],
						&node_32->chunks[index + 1],
						(node_32->b.b.count - index - 1) * sizeof(node_32->chunks[0]));

				node_32->chunks[node_32->b.b.count - 1] = BFM_TREE_NODE_INNER_32_INVALID;
				node_32->slots[node_32->b.b.count - 1] = NULL;

				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_inner_128 *node_128 =
					(bfm_tree_node_inner_128 *) node;
				uint8 offset;

				offset = node_128->offsets[child_chunk];
				Assert(offset != BFM_TREE_NODE_128_INVALID);
				Assert(node_128->slots[offset] == child);
				node_128->offsets[child_chunk] = BFM_TREE_NODE_128_INVALID;
				node_128->slots[offset] = NULL;
				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_inner_max *node_max =
					(bfm_tree_node_inner_max *) node;

				Assert(node_max->slots[child_chunk] == child);
				node_max->slots[child_chunk] = NULL;

				break;
			}
	}

	node->b.count--;

	if (node->b.count == 0)
	{
		if (node->b.parent)
			bfm_delete_inner(root, node->b.parent, &node->b, node->b.node_chunk);
		else
			root->rnode = NULL;
		bfm_free_inner(root, node);
	}
}

/*
 * NB: After this call node cannot be used anymore, it may have been freed or
 * shrunk.
 *
 * FIXME: this should implement shrinking of nodes
 */
static void pg_noinline
bfm_delete_leaf(bfm_tree * pg_restrict root, bfm_tree_node_leaf *pg_restrict node, int child_chunk)
{
	/* tell the compiler it doesn't need a bounds check */
	if ((bfm_tree_node_kind) node->b.kind > BFM_KIND_MAX)
		pg_unreachable();

	switch((bfm_tree_node_kind) node->b.kind)
	{
		case BFM_KIND_1:
			{
				bfm_tree_node_leaf_1 *node_1 =
					(bfm_tree_node_leaf_1 *) node;

				Assert(node_1->chunk == child_chunk);

				node_1->chunk = 17;
				break;
			}

		case BFM_KIND_4:
			{
				bfm_tree_node_leaf_4 *node_4 =
					(bfm_tree_node_leaf_4 *) node;
				int index;

				index = search_chunk_array_4_eq(node_4->chunks, child_chunk, node_4->b.b.count);
				Assert(index != -1);

				memmove(&node_4->values[index],
						&node_4->values[index + 1],
						(node_4->b.b.count - index - 1) * sizeof(node_4->values[0]));
				memmove(&node_4->chunks[index],
						&node_4->chunks[index + 1],
						(node_4->b.b.count - index - 1) * sizeof(node_4->chunks[0]));

				node_4->chunks[node_4->b.b.count - 1] = BFM_TREE_NODE_INNER_4_INVALID;
				node_4->values[node_4->b.b.count - 1] = 0xFF;

				break;
			}

		case BFM_KIND_16:
			{
				bfm_tree_node_leaf_16 *node_16 =
					(bfm_tree_node_leaf_16 *) node;
				int index;

				index = search_chunk_array_16_eq(node_16->chunks, child_chunk, node_16->b.b.count);
				Assert(index != -1);

				memmove(&node_16->values[index],
						&node_16->values[index + 1],
						(node_16->b.b.count - index - 1) * sizeof(node_16->values[0]));
				memmove(&node_16->chunks[index],
						&node_16->chunks[index + 1],
						(node_16->b.b.count - index - 1) * sizeof(node_16->chunks[0]));

				node_16->chunks[node_16->b.b.count - 1] = BFM_TREE_NODE_INNER_16_INVALID;
				node_16->values[node_16->b.b.count - 1] = 0xFF;

				break;
			}

		case BFM_KIND_32:
			{
				bfm_tree_node_leaf_32 *node_32 =
					(bfm_tree_node_leaf_32 *) node;
				int index;

				index = search_chunk_array_32_eq(node_32->chunks, child_chunk, node_32->b.b.count);
				Assert(index != -1);

				memmove(&node_32->values[index],
						&node_32->values[index + 1],
						(node_32->b.b.count - index - 1) * sizeof(node_32->values[0]));
				memmove(&node_32->chunks[index],
						&node_32->chunks[index + 1],
						(node_32->b.b.count - index - 1) * sizeof(node_32->chunks[0]));

				node_32->chunks[node_32->b.b.count - 1] = BFM_TREE_NODE_INNER_32_INVALID;
				node_32->values[node_32->b.b.count - 1] = 0xFF;

				break;
			}

		case BFM_KIND_128:
			{
				bfm_tree_node_leaf_128 *node_128 =
					(bfm_tree_node_leaf_128 *) node;

				Assert(node_128->offsets[child_chunk] != BFM_TREE_NODE_128_INVALID);
				node_128->offsets[child_chunk] = BFM_TREE_NODE_128_INVALID;
				break;
			}

		case BFM_KIND_MAX:
			{
				bfm_tree_node_leaf_max *node_max =
					(bfm_tree_node_leaf_max *) node;

				Assert(bfm_leaf_max_isset(node_max, child_chunk));
				bfm_leaf_max_unset(node_max, child_chunk);

				break;
			}
	}

#ifdef BFM_STATS
	root->entries--;
#endif
	node->b.count--;

	if (node->b.count == 0)
	{
		if (node->b.parent)
			bfm_delete_inner(root, node->b.parent, &node->b, node->b.node_chunk);
		else
			root->rnode = NULL;
		bfm_free_leaf(root, node);
	}
}

void
bfm_init(bfm_tree *root)
{
	memset(root, 0, sizeof(*root));

#if 1
	root->context = AllocSetContextCreate(CurrentMemoryContext, "radix bench internal",
										  ALLOCSET_DEFAULT_SIZES);
#else
	root->context = CurrentMemoryContext;
#endif

#ifdef BFM_USE_SLAB
	for (int i = 0; i < BFM_KIND_COUNT; i++)
	{
		root->inner_slabs[i] = SlabContextCreate(root->context,
												 inner_class_info[i].name,
												 Max(pg_nextpower2_32((MAXALIGN(inner_class_info[i].size) + 16) * 32), 1024),
												 inner_class_info[i].size);
		root->leaf_slabs[i] = SlabContextCreate(root->context,
												leaf_class_info[i].name,
												Max(pg_nextpower2_32((MAXALIGN(leaf_class_info[i].size) + 16) * 32), 1024),
												leaf_class_info[i].size);
#if 0
		elog(LOG, "%s %s size original %zu, mult %zu, round %u",
			 "leaf",
			 leaf_class_info[i].name,
			 leaf_class_info[i].size,
			 leaf_class_info[i].size * 32,
			 pg_nextpower2_32(leaf_class_info[i].size * 32));
#endif
	}
#endif

	/*
	 * XXX: Might be worth to always allocate a root node, to avoid related
	 * branches?
	 */
}

bool
bfm_lookup(bfm_tree *root, uint64_t key, bfm_value_type *val)
{
	bfm_tree_node *node;

	return bfm_walk(root, &node, val, key);
}

/*
 * Set key to val. Returns false if entry doesn't yet exist, true if it did.
 */
bool
bfm_set(bfm_tree *root, bfm_key_type key, bfm_value_type val)
{
	bfm_tree_node *cur;
	bfm_tree_node_leaf *target;
	uint8 chunk;
	uint32 shift;

	if (unlikely(!root->rnode))
		return bfm_set_empty(root, key, val);
	else if (key > root->maxval)
		return bfm_set_shallow(root, key, val);

	shift = root->rnode->node_shift;
	chunk = (key >> shift) & BFM_MASK;
	cur = root->rnode;

	while (shift > 0)
	{
		bfm_tree_node_inner *cur_inner;
		bfm_tree_node *slot;

		Assert(cur->node_shift > 0); /* leaf nodes look different */
		Assert(cur->node_shift == shift);

		cur_inner = (bfm_tree_node_inner *) cur;

		slot = bfm_find_one_level_inner(cur_inner, chunk);

		if (slot == NULL)
			return bfm_set_extend(root, key, val, cur_inner, shift, chunk);

		Assert(&slot->parent->b == cur);
		Assert(slot->node_chunk == chunk);

		cur = slot;
		shift -= BFM_FANOUT;
		chunk = (key >> shift) & BFM_MASK;
	}

	Assert(shift == 0 && cur->node_shift == 0);

	target = (bfm_tree_node_leaf *) cur;

	/*
	 * FIXME: what is the best API to deal with existing values? Overwrite?
	 * Overwrite and return old value? Just return true?
	 */
	return bfm_set_leaf(root, key, val, target, chunk);
}

bool
bfm_delete(bfm_tree *root, uint64 key)
{
	bfm_tree_node *node;
	bfm_value_type val;

	if (!bfm_walk(root, &node, &val, key))
		return false;

	Assert(node != NULL && node->node_shift == 0);

	/* recurses upwards and deletes parent nodes if necessary */
	bfm_delete_leaf(root, (bfm_tree_node_leaf *) node, key & BFM_MASK);

	return true;
}


StringInfo
bfm_stats(bfm_tree *root)
{
	StringInfo s;
#ifdef BFM_STATS
	size_t total;
	size_t inner_bytes;
	size_t leaf_bytes;
	size_t allocator_bytes;
#endif

	s = makeStringInfo();

	/* FIXME: Some of the below could be printed even without BFM_STATS */
#ifdef BFM_STATS
	appendStringInfo(s, "%zu entries and depth %d\n",
					 root->entries,
					 root->rnode ? root->rnode->node_shift / BFM_FANOUT : 0);

	{
		appendStringInfo(s, "\tinner nodes:");
		total = 0;
		inner_bytes = 0;
		for (int i = 0; i < BFM_KIND_COUNT; i++)
		{
			total += root->inner_nodes[i];
			inner_bytes += inner_class_info[i].size * root->inner_nodes[i];
			appendStringInfo(s, " %s: %zu, ",
							 inner_class_info[i].name,
							 root->inner_nodes[i]);
		}
		appendStringInfo(s, " total: %zu, total_bytes: %zu\n", total,
						 inner_bytes);
	}

	{
		appendStringInfo(s, "\tleaf nodes:");
		total = 0;
		leaf_bytes = 0;
		for (int i = 0; i < BFM_KIND_COUNT; i++)
		{
			total += root->leaf_nodes[i];
			leaf_bytes += leaf_class_info[i].size * root->leaf_nodes[i];
			appendStringInfo(s, " %s: %zu, ",
							 leaf_class_info[i].name,
							 root->leaf_nodes[i]);
		}
		appendStringInfo(s, " total: %zu, total_bytes: %zu\n", total,
						 leaf_bytes);
	}

	allocator_bytes = MemoryContextMemAllocated(root->context, true);

	appendStringInfo(s, "\t%.2f MB excluding allocator overhead, %.2f MiB including\n",
					 (inner_bytes + leaf_bytes) / (double) (1024 * 1024),
					 allocator_bytes / (double) (1024 * 1024));
	appendStringInfo(s, "\t%.2f bytes/entry excluding allocator overhead\n",
					 root->entries > 0 ?
					 (inner_bytes + leaf_bytes)/(double)root->entries : 0);
	appendStringInfo(s, "\t%.2f bytes/entry including allocator overhead\n",
					 root->entries > 0 ?
					 allocator_bytes/(double)root->entries : 0);
#endif

	if (0)
		bfm_print(root);

	return s;
}

static void
bfm_print_node(StringInfo s, int indent, bfm_value_type key, bfm_tree_node *node);

static void
bfm_print_node_child(StringInfo s, int indent, bfm_value_type key, bfm_tree_node *node,
					 int i, uint8 chunk, bfm_tree_node *child)
{
	appendStringInfoSpaces(s, indent + 2);
	appendStringInfo(s, "%u: child chunk: 0x%.2X, child: %p\n",
					 i, chunk, child);
	key |= ((uint64) chunk) << node->node_shift;

	bfm_print_node(s, indent + 4, key, child);
}

static void
bfm_print_value(StringInfo s, int indent, bfm_value_type key, bfm_tree_node *node,
				int i, uint8 chunk, bfm_value_type value)
{
	key |= chunk;

	appendStringInfoSpaces(s, indent + 2);
	appendStringInfo(s, "%u: chunk: 0x%.2X, key: 0x%llX/%llu, value: 0x%llX/%llu\n",
					 i,
					 chunk,
					 (unsigned long long) key,
					 (unsigned long long) key,
					 (unsigned long long) value,
					 (unsigned long long) value);
}

static void
bfm_print_node(StringInfo s, int indent, bfm_value_type key, bfm_tree_node *node)
{
	appendStringInfoSpaces(s, indent);
	appendStringInfo(s, "%s: kind %d, children: %u, shift: %u, node chunk: 0x%.2X, partial key: 0x%llX\n",
					 node->node_shift != 0 ? "inner" : "leaf",
					 node->kind,
					 node->count,
					 node->node_shift,
					 node->node_chunk,
					 (long long unsigned) key);

	if (node->node_shift != 0)
	{
		bfm_tree_node_inner *inner = (bfm_tree_node_inner *) node;

		switch((bfm_tree_node_kind) inner->b.kind)
		{
			case BFM_KIND_1:
				{
					bfm_tree_node_inner_1 *node_1 =
						(bfm_tree_node_inner_1 *) node;

					if (node_1->b.b.count > 0)
						bfm_print_node_child(s, indent, key, node,
											 0, node_1->chunk, node_1->slot);

					break;
				}

			case BFM_KIND_4:
				{
					bfm_tree_node_inner_4 *node_4 =
						(bfm_tree_node_inner_4 *) node;

					for (int i = 0; i < node_4->b.b.count; i++)
					{
						bfm_print_node_child(s, indent, key, node,
											 i, node_4->chunks[i], node_4->slots[i]);
					}

					break;
				}

			case BFM_KIND_16:
				{
					bfm_tree_node_inner_16 *node_16 =
						(bfm_tree_node_inner_16 *) node;

					for (int i = 0; i < node_16->b.b.count; i++)
					{
						bfm_print_node_child(s, indent, key, node,
											 i, node_16->chunks[i], node_16->slots[i]);
					}

					break;
				}

			case BFM_KIND_32:
				{
					bfm_tree_node_inner_32 *node_32 =
						(bfm_tree_node_inner_32 *) node;

					for (int i = 0; i < node_32->b.b.count; i++)
					{
						bfm_print_node_child(s, indent, key, node,
											 i, node_32->chunks[i], node_32->slots[i]);
					}

					break;
				}

			case BFM_KIND_128:
				{
					bfm_tree_node_inner_128 *node_128 =
						(bfm_tree_node_inner_128 *) node;

					for (int i = 0; i < BFM_MAX_CLASS; i++)
					{
						uint8 offset = node_128->offsets[i];

						if (offset == BFM_TREE_NODE_128_INVALID)
							continue;

						bfm_print_node_child(s, indent, key, node,
											 offset, i, node_128->slots[offset]);
					}

					break;
				}

			case BFM_KIND_MAX:
				{
					bfm_tree_node_inner_max *node_max =
						(bfm_tree_node_inner_max *) node;

					for (int i = 0; i < BFM_MAX_CLASS; i++)
					{
						if (node_max->slots[i] == NULL)
							continue;

						bfm_print_node_child(s, indent, key, node,
											 i, i, node_max->slots[i]);
					}

					break;
				}
		}
	}
	else
	{
		bfm_tree_node_leaf *leaf = (bfm_tree_node_leaf *) node;

		switch((bfm_tree_node_kind) leaf->b.kind)
		{
			case BFM_KIND_1:
				{
					bfm_tree_node_leaf_1 *node_1 =
						(bfm_tree_node_leaf_1 *) node;

					if (node_1->b.b.count > 0)
						bfm_print_value(s, indent, key, node,
										0, node_1->chunk, node_1->value);

					break;
				}

			case BFM_KIND_4:
				{
					bfm_tree_node_leaf_4 *node_4 =
						(bfm_tree_node_leaf_4 *) node;

					for (int i = 0; i < node_4->b.b.count; i++)
					{
						bfm_print_value(s, indent, key, node,
										i, node_4->chunks[i], node_4->values[i]);
					}

					break;
				}

			case BFM_KIND_16:
				{
					bfm_tree_node_leaf_16 *node_16 =
						(bfm_tree_node_leaf_16 *) node;

					for (int i = 0; i < node_16->b.b.count; i++)
					{
						bfm_print_value(s, indent, key, node,
										i, node_16->chunks[i], node_16->values[i]);
					}

					break;
				}

			case BFM_KIND_32:
				{
					bfm_tree_node_leaf_32 *node_32 =
						(bfm_tree_node_leaf_32 *) node;

					for (int i = 0; i < node_32->b.b.count; i++)
					{
						bfm_print_value(s, indent, key, node,
										i, node_32->chunks[i], node_32->values[i]);
					}

					break;
				}

			case BFM_KIND_128:
				{
					bfm_tree_node_leaf_128 *node_128 =
						(bfm_tree_node_leaf_128 *) node;

					for (int i = 0; i < BFM_MAX_CLASS; i++)
					{
						uint8 offset = node_128->offsets[i];

						if (offset == BFM_TREE_NODE_128_INVALID)
							continue;

						bfm_print_value(s, indent, key, node,
										offset, i, node_128->values[offset]);
					}

					break;
				}

			case BFM_KIND_MAX:
				{
					bfm_tree_node_leaf_max *node_max =
						(bfm_tree_node_leaf_max *) node;

					for (int i = 0; i < BFM_MAX_CLASS; i++)
					{
						if (!bfm_leaf_max_isset(node_max, i))
							continue;

						bfm_print_value(s, indent, key, node,
										i, i, node_max->values[i]);
					}

					break;
				}
		}
	}
}

void
bfm_print(bfm_tree *root)
{
	StringInfoData s;

	initStringInfo(&s);

	if (root->rnode)
		bfm_print_node(&s, 0 /* indent */, 0 /* key */, root->rnode);

	elog(LOG, "radix debug print:\n%s", s.data);
	pfree(s.data);
}


#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, \
				 "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_FALSE(expr)	\
	do { \
		if (expr) \
			elog(ERROR, \
				 "%s was unexpectedly true in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_U32(result_expr, expected_expr)	\
	do { \
		uint32		result = (result_expr); \
		uint32		expected = (expected_expr); \
		if (result != expected) \
			elog(ERROR, \
				 "%s yielded %u, expected %s in file \"%s\" line %u", \
				 #result_expr, result, #expected_expr, __FILE__, __LINE__); \
	} while (0)

static void
bfm_test_insert_leaf_grow(bfm_tree *root)
{
	bfm_value_type val;

	/* 0->1 */
	EXPECT_FALSE(bfm_set(root, 0, 0+3));
	EXPECT_TRUE(bfm_lookup(root, 0, &val));
	EXPECT_EQ_U32(val, 0+3);

	/* node 1->4 */
	for (int i = 1; i < 4; i++)
	{
		EXPECT_FALSE(bfm_set(root, i, i+3));
	}
	for (int i = 0; i < 4; i++)
	{
		EXPECT_TRUE(bfm_lookup(root, i, &val));
		EXPECT_EQ_U32(val, i+3);
	}

	/* node 4->16, reverse order, for giggles */
	for (int i = 15; i >= 4; i--)
	{
		EXPECT_FALSE(bfm_set(root, i, i+3));
	}
	for (int i = 0; i < 16; i++)
	{
		EXPECT_TRUE(bfm_lookup(root, i, &val));
		EXPECT_EQ_U32(val, i+3);
	}

	/* node 16->32 */
	for (int i = 16; i < 32; i++)
	{
		EXPECT_FALSE(bfm_set(root, i, i+3));
	}
	for (int i = 0; i < 32; i++)
	{
		EXPECT_TRUE(bfm_lookup(root, i, &val));
		EXPECT_EQ_U32(val, i+3);
	}

	/* node 32->128 */
	for (int i = 32; i < 128; i++)
	{
		EXPECT_FALSE(bfm_set(root, i, i+3));
	}
	for (int i = 0; i < 128; i++)
	{
		EXPECT_TRUE(bfm_lookup(root, i, &val));
		EXPECT_EQ_U32(val, i+3);
	}

	/* node 128->max */
	for (int i = 128; i < BFM_MAX_CLASS; i++)
	{
		EXPECT_FALSE(bfm_set(root, i, i+3));
	}
	for (int i = 0; i < BFM_MAX_CLASS; i++)
	{
		EXPECT_TRUE(bfm_lookup(root, i, &val));
		EXPECT_EQ_U32(val, i+3);
	}

}

static void
bfm_test_insert_inner_grow(void)
{
	bfm_tree root;
	bfm_value_type val;
	bfm_value_type cur;

	bfm_init(&root);

	cur = 1025;

	while (!root.rnode ||
		   root.rnode->node_shift == 0 ||
		   root.rnode->count < 4)
	{
		EXPECT_FALSE(bfm_set(&root, cur, -cur));
		cur += BFM_MAX_CLASS;
	}

	for (int i = 1025; i < cur; i += BFM_MAX_CLASS)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
	}

	while (root.rnode->count < 32)
	{
		EXPECT_FALSE(bfm_set(&root, cur, -cur));
		cur += BFM_MAX_CLASS;
	}

	for (int i = 1025; i < cur; i += BFM_MAX_CLASS)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
	}

	while (root.rnode->count < 128)
	{
		EXPECT_FALSE(bfm_set(&root, cur, -cur));
		cur += BFM_MAX_CLASS;
	}

	for (int i = 1025; i < cur; i += BFM_MAX_CLASS)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
	}

	while (root.rnode->count < BFM_MAX_CLASS)
	{
		EXPECT_FALSE(bfm_set(&root, cur, -cur));
		cur += BFM_MAX_CLASS;
	}

	for (int i = 1025; i < cur; i += BFM_MAX_CLASS)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
	}

	while (root.rnode->count == BFM_MAX_CLASS)
	{
		EXPECT_FALSE(bfm_set(&root, cur, -cur));
		cur += BFM_MAX_CLASS;
	}

	for (int i = 1025; i < cur; i += BFM_MAX_CLASS)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
	}

}

static void
bfm_test_delete_lots(void)
{
	bfm_tree root;
	bfm_value_type val;
	bfm_key_type insertval;

	bfm_init(&root);

	insertval = 0;
	while (!root.rnode ||
		   root.rnode->node_shift != (BFM_FANOUT * 2))
	{
		EXPECT_FALSE(bfm_set(&root, insertval, -insertval));
		insertval++;
	}

	for (bfm_key_type i = 0; i < insertval; i++)
	{
		EXPECT_TRUE(bfm_lookup(&root, i, &val));
		EXPECT_EQ_U32(val, -i);
		EXPECT_TRUE(bfm_delete(&root, i));
		EXPECT_FALSE(bfm_lookup(&root, i, &val));
	}

	EXPECT_TRUE(root.rnode == NULL);
}

#include "portability/instr_time.h"

static void
bfm_test_insert_bulk(int count)
{
	bfm_tree root;
	bfm_value_type val;
	instr_time start, end, diff;
	int misses;
	int mult = 1;

	bfm_init(&root);

	INSTR_TIME_SET_CURRENT(start);

	for (int i = 0; i < count; i++)
		bfm_set(&root, i*mult, -i);

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SET_ZERO(diff);
	INSTR_TIME_ACCUM_DIFF(diff, end, start);

	elog(NOTICE, "%d ordered insertions in %f seconds, %d/sec",
		 count,
		 INSTR_TIME_GET_DOUBLE(diff),
		 (int)(count/INSTR_TIME_GET_DOUBLE(diff)));

	INSTR_TIME_SET_CURRENT(start);

	misses = 0;
	for (int i = 0; i < count; i++)
	{
		if (unlikely(!bfm_lookup(&root, i*mult, &val)))
			misses++;
	}
	if (misses > 0)
		elog(ERROR, "not present for lookup: %d entries", misses);

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SET_ZERO(diff);
	INSTR_TIME_ACCUM_DIFF(diff, end, start);

	elog(NOTICE, "%d ordered lookups in %f seconds, %d/sec",
		 count,
		 INSTR_TIME_GET_DOUBLE(diff),
		 (int)(count/INSTR_TIME_GET_DOUBLE(diff)));

	elog(LOG, "stats after lookup are: %s",
		 bfm_stats(&root)->data);

	INSTR_TIME_SET_CURRENT(start);

	misses = 0;
	for (int i = 0; i < count; i++)
	{
		if (unlikely(!bfm_delete(&root, i*mult)))
			misses++;
	}
	if (misses > 0)
		elog(ERROR, "not present for deletion: %d entries", misses);

	INSTR_TIME_SET_CURRENT(end);
	INSTR_TIME_SET_ZERO(diff);
	INSTR_TIME_ACCUM_DIFF(diff, end, start);

	elog(NOTICE, "%d ordered deletions in %f seconds, %d/sec",
		 count,
		 INSTR_TIME_GET_DOUBLE(diff),
		 (int)(count/INSTR_TIME_GET_DOUBLE(diff)));

	elog(LOG, "stats after deletion are: %s",
		 bfm_stats(&root)->data);
}

void
bfm_tests(void)
{
	bfm_tree root;
	bfm_value_type val;

	/* initialize a tree starting with a large value */
	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 1024, 1));
	EXPECT_TRUE(bfm_lookup(&root, 1024, &val));
	EXPECT_EQ_U32(val, 1);
	/* there should only be the key we inserted */
#ifdef BFM_STATS
	EXPECT_EQ_U32(root.leaf_nodes[0], 1);
#endif

	/* check that we can subsequently insert a small value */
	EXPECT_FALSE(bfm_set(&root, 1, 2));
	EXPECT_TRUE(bfm_lookup(&root, 1, &val));
	EXPECT_EQ_U32(val, 2);
	EXPECT_TRUE(bfm_lookup(&root, 1024, &val));
	EXPECT_EQ_U32(val, 1);

	/* check that a 0 key and 0 value are correctly recognized */
	bfm_init(&root);
	EXPECT_FALSE(bfm_lookup(&root, 0, &val));
	EXPECT_FALSE(bfm_set(&root, 0, 17));
	EXPECT_TRUE(bfm_lookup(&root, 0, &val));
	EXPECT_EQ_U32(val, 17);

	EXPECT_FALSE(bfm_lookup(&root, 2, &val));
	EXPECT_FALSE(bfm_set(&root, 2, 0));
	EXPECT_TRUE(bfm_lookup(&root, 2, &val));
	EXPECT_EQ_U32(val, 0);

	/* check that repeated insertion of the same key updates value */
	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 9, 12));
	EXPECT_TRUE(bfm_lookup(&root, 9, &val));
	EXPECT_EQ_U32(val, 12);
	EXPECT_TRUE(bfm_set(&root, 9, 13));
	EXPECT_TRUE(bfm_lookup(&root, 9, &val));
	EXPECT_EQ_U32(val, 13);


	/* initialize a tree starting with a leaf value */
	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 3, 1));
	EXPECT_TRUE(bfm_lookup(&root, 3, &val));
	EXPECT_EQ_U32(val, 1);
	/* there should only be the key we inserted */
#ifdef BFM_STATS
	EXPECT_EQ_U32(root.leaf_nodes[0], 1);
#endif
	/* and no inner ones */
#ifdef BFM_STATS
	EXPECT_EQ_U32(root.inner_nodes[0], 0);
#endif

	EXPECT_FALSE(bfm_set(&root, 1717, 17));
	EXPECT_TRUE(bfm_lookup(&root, 1717, &val));
	EXPECT_EQ_U32(val, 17);

	/* check that a root leaf node grows correctly */
	bfm_init(&root);
	bfm_test_insert_leaf_grow(&root);

	/* check that a non-root leaf node grows correctly */
	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 1024, 1024));
	bfm_test_insert_leaf_grow(&root);

	/* check that an inner node grows correctly */
	bfm_test_insert_inner_grow();


	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 1, 1));
	EXPECT_TRUE(bfm_lookup(&root, 1, &val));

	/* deletion from leaf node at root */
	EXPECT_TRUE(bfm_delete(&root, 1));
	EXPECT_FALSE(bfm_lookup(&root, 1, &val));

	/* repeated deletion fails */
	EXPECT_FALSE(bfm_delete(&root, 1));
	EXPECT_TRUE(root.rnode == NULL);

	/* one deletion doesn't disturb other values in leaf */
	EXPECT_FALSE(bfm_set(&root, 1, 1));
	EXPECT_FALSE(bfm_set(&root, 2, 2));
	EXPECT_TRUE(bfm_delete(&root, 1));
	EXPECT_FALSE(bfm_lookup(&root, 1, &val));
	EXPECT_TRUE(bfm_lookup(&root, 2, &val));
	EXPECT_EQ_U32(val, 2);

	EXPECT_TRUE(bfm_delete(&root, 2));
	EXPECT_FALSE(bfm_lookup(&root, 2, &val));
	EXPECT_TRUE(root.rnode == NULL);

	/* deletion from a leaf node succeeds */
	EXPECT_FALSE(bfm_set(&root, 0xFFFF02, 0xFFFF02));
	EXPECT_FALSE(bfm_set(&root, 1, 1));
	EXPECT_FALSE(bfm_set(&root, 2, 2));

	EXPECT_TRUE(bfm_delete(&root, 1));
	EXPECT_TRUE(bfm_lookup(&root, 0xFFFF02, &val));
	EXPECT_FALSE(bfm_lookup(&root, 1, &val));
	EXPECT_TRUE(bfm_lookup(&root, 2, &val));

	EXPECT_TRUE(bfm_delete(&root, 2));
	EXPECT_TRUE(bfm_lookup(&root, 0xFFFF02, &val));
	EXPECT_FALSE(bfm_lookup(&root, 1, &val));

	EXPECT_TRUE(bfm_delete(&root, 0xFFFF02));
	EXPECT_FALSE(bfm_delete(&root, 0xFFFF02));
	EXPECT_FALSE(bfm_lookup(&root, 0xFFFF02, &val));
	EXPECT_TRUE(root.rnode == NULL);

	/* check that repeatedly inserting and deleting the same value works */
	bfm_init(&root);
	EXPECT_FALSE(bfm_set(&root, 0x10000, -0x10000));
	EXPECT_FALSE(bfm_set(&root, 0, 0));
	EXPECT_TRUE(bfm_lookup(&root, 0, &val));
	EXPECT_TRUE(bfm_delete(&root, 0));
	EXPECT_FALSE(bfm_lookup(&root, 0, &val));
	EXPECT_FALSE(bfm_set(&root, 0, 0));
	EXPECT_TRUE(bfm_set(&root, 0, 0));
	EXPECT_TRUE(bfm_lookup(&root, 0, &val));

	bfm_test_delete_lots();

	if (0)
	{
		int cnt = 300;

		bfm_init(&root);
		MemoryContextStats(root.context);
		for (int i = 0; i < cnt; i++)
			EXPECT_FALSE(bfm_set(&root, i, i));
		MemoryContextStats(root.context);
		for (int i = 0; i < cnt; i++)
			EXPECT_TRUE(bfm_delete(&root, i));
		MemoryContextStats(root.context);
	}

	if (1)
	{
		//bfm_test_insert_bulk(        100 * 1000);
		//bfm_test_insert_bulk(       1000 * 1000);
#ifdef USE_ASSERT_CHECKING
		bfm_test_insert_bulk(   1 * 1000 * 1000);
#endif
		//bfm_test_insert_bulk(  10 * 1000 * 1000);
#ifndef USE_ASSERT_CHECKING
		bfm_test_insert_bulk( 100 * 1000 * 1000);
#endif
		//bfm_test_insert_bulk(1000 * 1000 * 1000);
	}

	//bfm_print(&root);
}
