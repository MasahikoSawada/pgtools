#include "postgres.h"

#include "miscadmin.h"
#include "port/pg_bitutils.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"

#include "radix_tree.h"

#define RADIX_TREE_NODE_FANOUT	8
#define RADIX_TREE_CHUNK_MASK ((1 << RADIX_TREE_NODE_FANOUT) - 1)
#define RADIX_TREE_MAX_SHIFT key_get_shift(UINT64_MAX)

#define IsLeaf(n) (((radix_tree_node *) (n))->shift == 0)
#define Node48IdxIsUsed(idx) (idx == ((1 << 8) - 1))

typedef enum radix_tree_node_kind
{
	RADIX_TREE_NODE_KIND_4 = 0,
	RADIX_TREE_NODE_KIND_16,
	RADIX_TREE_NODE_KIND_48,
	RADIX_TREE_NODE_KIND_256
} radix_tree_node_kind;
#define RADIX_TREE_NODE_KIND_COUNT 4

typedef struct radix_tree_node
{
	uint8	count;
	uint8	shift;
	uint8	chunk;
	radix_tree_node_kind	kind;
} radix_tree_node;

typedef struct radix_tree_node_4
{
	radix_tree_node n;

	uint8	chunks[4];
	Datum	slots[4];
	//radix_tree_node *slots[4];
} radix_tree_node_4;

typedef struct radix_tree_node_16
{
	radix_tree_node n;

	uint8	chunks[16];
	Datum slots[16];
	//radix_tree_node *slots[16];
} radix_tree_node_16;

typedef struct radix_tree_node_48
{
	radix_tree_node n;

	uint8	slot_idxs[255];
	Datum slots[48];
	//radix_tree_node *slots[48];
} radix_tree_node_48;

typedef struct radix_tree_node_256
{
	radix_tree_node n;

	Datum	slots[256];
	//radix_tree_node *slots[256];
} radix_tree_node_256;

typedef struct radix_tree_node_info_elem
{
	const char *name;
	int		nslots;
	Size	size;
} radix_tree_node_info_elem;

static radix_tree_node_info_elem radix_tree_node_info[] =
{
	{"radix tree node 4", 4, sizeof(radix_tree_node_4)},
	{"radix tree node 16", 16, sizeof(radix_tree_node_16)},
	{"radix tree node 48", 48, sizeof(radix_tree_node_48)},
	{"radix tree node 256", 256, sizeof(radix_tree_node_256)},
};

struct radix_tree
{
	uint64	max_val;
	MemoryContext context;
	radix_tree_node	*root;
	MemoryContextData *slabs[RADIX_TREE_NODE_KIND_COUNT];

	/* stats */
	int32	cnt[RADIX_TREE_NODE_KIND_COUNT];
	uint64 nkeys;
};

static radix_tree_node *radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node);
static radix_tree_node *radix_tree_find_child(radix_tree_node *node, uint64 key);
static Datum *radix_tree_find_slot_ptr(radix_tree_node *node, uint8 chunk);
static void radix_tree_replace_slot(radix_tree_node *parent, radix_tree_node *node,
									uint8 chunk);
static void radix_tree_extend(radix_tree *tree, uint64 key);
static void radix_tree_new_root(radix_tree *tree, uint64 key, Datum val);
static radix_tree_node *radix_tree_insert_child(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
												uint64 key);
static void radix_tree_insert_val(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
								  uint64 key, Datum val);

#define GET_KEY_CHUNK(key, shift) \
	((uint8) (((key) >> (shift)) & RADIX_TREE_CHUNK_MASK))

/*
 * Return the shift that is satisfied to store the given key.
 */
inline static int
key_get_shift(uint64 key)
{
	return (key == 0)
		? 0
		: (pg_leftmost_one_pos64(key) / RADIX_TREE_NODE_FANOUT) * RADIX_TREE_NODE_FANOUT;
}

/*
 * Return the max value stored in a node with the given shift.
 */
static uint64
shift_get_max_val(int shift)
{
	if (shift == RADIX_TREE_MAX_SHIFT)
		return UINT64_MAX;

	return (UINT64_C(1) << (shift + RADIX_TREE_NODE_FANOUT)) - 1;
}

static radix_tree_node *
radix_tree_alloc_node(radix_tree *tree, radix_tree_node_kind kind)
{
	radix_tree_node *newnode;

	newnode = (radix_tree_node *) MemoryContextAllocZero(tree->slabs[kind],
														 radix_tree_node_info[kind].size);
	newnode->kind = kind;

	/* stats */
	tree->cnt[kind]++;

	return newnode;
}

static void
radix_tree_copy_node_common(radix_tree_node *src, radix_tree_node *dst)
{
	dst->shift = src->shift;
	dst->chunk = src->chunk;
	dst->count = src->count;
}

static void
radix_tree_free_node(radix_tree_node *node)
{
	pfree(node);
}

/*
 * The tree doesn't have not sufficient height, so grow it */
static void
radix_tree_extend(radix_tree *tree, uint64 key)
{
	int max_shift;
	int shift = tree->root->shift + RADIX_TREE_NODE_FANOUT;

	max_shift = key_get_shift(key);

	/* Grow from 'shift' to 'max_shift' */
	while (shift <= max_shift)
	{
		radix_tree_node_4 *node =
			(radix_tree_node_4 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);
		uint8	chunk = GET_KEY_CHUNK(key, shift);

		node->n.count = 1;
		node->n.shift = shift;
		node->chunks[0] = 0;
		node->slots[0] = PointerGetDatum(tree->root);

		tree->root->chunk = 0;
		tree->root = (radix_tree_node *) node;

		shift += RADIX_TREE_NODE_FANOUT;
	}

	tree->max_val = shift_get_max_val(max_shift);
}


/*
 * Return the pointer to the child node corresponding with the key. Otherwise (if
 * not found) return NULL.
 */
static radix_tree_node *
radix_tree_find_child(radix_tree_node *node, uint64 key)
{
	Datum *slot_ptr;
	int chunk = GET_KEY_CHUNK(key, node->shift);

	slot_ptr = radix_tree_find_slot_ptr(node, chunk);

	return (slot_ptr == NULL) ? NULL : (radix_tree_node *) DatumGetPointer(*slot_ptr);
}

/*
 * Return the address of the slot corresponding to chunk in the node, if found.
 * Otherwise return NULL.
 */
static Datum *
radix_tree_find_slot_ptr(radix_tree_node *node, uint8 chunk)
{

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
			for (int i = 0; i < n4->n.count; i++)
			{
				if (n4->chunks[i] == chunk)
					return &(n4->slots[i]);
			}

			return NULL;

			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;
			for (int i = 0; i < n16->n.count; i++)
			{
				if (n16->chunks[i] == chunk)
					return &(n16->slots[i]);
			}

			return NULL;

			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;
			int pos = n48->slot_idxs[chunk] - 1;

			if (pos < 0)
				return NULL;

			return &(n48->slots[pos]);
			break;
		}
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

			return &(n256->slots[chunk]);
			break;
		}
	}

	pg_unreachable();
}

/* Redirect from the parent to the node */
static void
radix_tree_replace_slot(radix_tree_node *parent, radix_tree_node *node, uint8 chunk)
{
	Datum *slot_ptr;

	slot_ptr = radix_tree_find_slot_ptr(parent, chunk);
	*slot_ptr = PointerGetDatum(node);
}

/*
 * Create a new node as the root. Subordinate nodes will be created during
 * the insertion.
 */
static void
radix_tree_new_root(radix_tree *tree, uint64 key, Datum val)
{
	int chunk = key & RADIX_TREE_CHUNK_MASK;
	radix_tree_node_4 * n4 =
		(radix_tree_node_4 * ) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);
	int shift = key_get_shift(key);

	n4->n.shift = shift;
	tree->max_val = shift_get_max_val(shift);
	tree->root = (radix_tree_node *) n4;
}

static radix_tree_node *
radix_tree_insert_child(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
						uint64 key)
{
	radix_tree_node *newchild =
		(radix_tree_node *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_4);

	Assert(!IsLeaf(node));

	newchild->shift = node->shift - RADIX_TREE_NODE_FANOUT;
	newchild->chunk = GET_KEY_CHUNK(key, node->shift);

	radix_tree_insert_val(tree, parent, node, key, PointerGetDatum(newchild));

	return (radix_tree_node *) newchild;
}

static void
radix_tree_insert_val(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node,
					  uint64 key, Datum val)
{
	int chunk = GET_KEY_CHUNK(key, node->shift);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;

			if (n4->n.count < 4)
			{
				n4->chunks[n4->n.count] = chunk;
				n4->slots[n4->n.count] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_16);
			/* fall through */
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;

			if (n16->n.count < 16)
			{
				int i;
				for (i = 0; i < n16->n.count; i++)
				{
					if (n16->chunks[i] > chunk)
					{
						memmove(&(n16->chunks[i + 1]), &(n16->chunks[i]),
								sizeof(uint8) * (n16->n.count - i));
						memmove(&(n16->slots[i + 1]), &(n16->slots[i]),
								sizeof(radix_tree_node *) * (n16->n.count - i));
						break;
					}
				}

				n16->chunks[i] = chunk;
				n16->slots[i] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_48);
			/* fall through */
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;

			if (n48->n.count < 48)
			{
				uint8 pos = n48->n.count + 1;
				n48->slot_idxs[chunk] = pos;
				n48->slots[pos - 1] = val;
				break;
			}

			node = radix_tree_node_grow(tree, parent, node);
			Assert(node->kind == RADIX_TREE_NODE_KIND_256);
			/* fall through */
		}
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

			Assert(n256->n.count <= 255);
			n256->slots[chunk] = val;
			break;
		}
	}

	if (node->count < 255)
		node->count++;
}



static radix_tree_node *
radix_tree_node_grow(radix_tree *tree, radix_tree_node *parent, radix_tree_node *node)
{
	radix_tree_node *newnode;

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;
			radix_tree_node_16 *new16 =
				(radix_tree_node_16 *) radix_tree_alloc_node(tree, RADIX_TREE_NODE_KIND_16);

			Assert(n4->n.count == 4);

			radix_tree_copy_node_common((radix_tree_node *) n4,
										(radix_tree_node *) new16);

			memcpy(&(new16->chunks), &(n4->chunks), sizeof(uint8) * 4);
			memcpy(&(new16->slots), &(n4->slots), sizeof(Datum) * 4);

			for (int i = 0; i < n4->n.count ; i++)
			{
				for (int j = i; j < n4->n.count; j++)
				{
					if (new16->chunks[i] > new16->chunks[j])
					{
						new16->chunks[i] = new16->chunks[j];
						new16->slots[i] = new16->slots[j];
					}
				}
			}

			/* TEST */
			{
				for (int i = 1; i < new16->n.count; i++)
					Assert(new16->chunks[i - 1] < new16->chunks[i]);
			}

			/* @@@ sort chunks */

			newnode = (radix_tree_node *) new16;
			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;
			radix_tree_node_48 *new48 =
				(radix_tree_node_48 *) radix_tree_alloc_node(tree,RADIX_TREE_NODE_KIND_48);

			Assert(n16->n.count == 16);

			radix_tree_copy_node_common((radix_tree_node *) n16,
										(radix_tree_node *) new48);

			for (int i = 0; i < n16->n.count; i++)
			{
				new48->slot_idxs[n16->chunks[i]] = i + 1;
				new48->slots[i] = n16->slots[i];
			}

			/* TEST */
			{
				for (int i = 0; i < n16->n.count; i++)
				{
					uint8 chunk = n16->chunks[i];

					Assert(new48->slot_idxs[chunk] != 0);
				}
			}

			newnode = (radix_tree_node *) new48;
			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;
			radix_tree_node_256 *new256 =
				(radix_tree_node_256 *) radix_tree_alloc_node(tree,RADIX_TREE_NODE_KIND_256);

			Assert(n48->n.count == 48);

			radix_tree_copy_node_common((radix_tree_node *) n48,
										(radix_tree_node *) new256);

			for (int i = 0; i < 256; i++)
			{
				int idx = n48->slot_idxs[i];

				if (idx == 0)
					continue;

				new256->slots[i] = n48->slots[idx - 1];
			}

			newnode = (radix_tree_node *) new256;
			break;
		}
		case RADIX_TREE_NODE_KIND_256:
			elog(ERROR, "radix tree node_256 cannot be grew");
			break;
	}

	if (parent == node)
		tree->root = newnode;
	else
		radix_tree_replace_slot(parent, newnode, node->chunk);

	tree->cnt[node->kind]--;
	radix_tree_free_node(node);

	return newnode;
}

radix_tree *
radix_tree_create(MemoryContext ctx)
{
	radix_tree *tree;
	MemoryContext old_ctx;

	old_ctx = MemoryContextSwitchTo(ctx);

	tree = palloc(sizeof(radix_tree));
	tree->max_val = 0;
	tree->root = NULL;
	tree->context = ctx;

	/* stats */
	tree->nkeys = 0;

	for (int i = 0; i < RADIX_TREE_NODE_KIND_COUNT; i++)
		tree->slabs[i] = SlabContextCreate(ctx,
										   radix_tree_node_info[i].name,
										   SLAB_DEFAULT_BLOCK_SIZE,
										   radix_tree_node_info[i].size);

	MemoryContextSwitchTo(old_ctx);

	return tree;
}

void
radix_tree_destroy(radix_tree *tree)
{
	for (int i = 0; i < RADIX_TREE_NODE_KIND_COUNT; i++)
		MemoryContextDelete(tree->slabs[i]);

	pfree(tree);
}

bool
radix_tree_insert(radix_tree *tree, uint64 key, Datum val)
{
	int shift;
	radix_tree_node *node;
	radix_tree_node *parent = tree->root;
	bool leaf_inserted = false;

	/* stats */
	tree->nkeys++;

	/* Empty tree, create new leaf node as the root */
	if (!tree->root)
		radix_tree_new_root(tree, key, val);

	if (key > tree->max_val)
		radix_tree_extend(tree, key);

	Assert(tree->root);

	shift = tree->root->shift;
	node = tree->root;
	while (shift > 0)
	{
		radix_tree_node *child;

		child = radix_tree_find_child(node, key);

		if (child == NULL)
			child = radix_tree_insert_child(tree, parent, node, key);

		parent = node;
		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

	/* arrived at a leaf */
	Assert(IsLeaf(node));

	radix_tree_insert_val(tree, parent, node, key, val);

	return true;
}

Datum
radix_tree_search(radix_tree *tree, uint64 key, bool *found)
{
	radix_tree_node *node;
	int shift;

	if (!tree->root || key > tree->max_val)
		goto not_found;

	node = tree->root;
	shift = tree->root->shift;
	while (shift >= 0)
	{
		radix_tree_node *child;

		if (IsLeaf(node))
		{
			Datum *slot_ptr;
			int chunk = GET_KEY_CHUNK(key, node->shift);

			/* We reached at a leaf node, find the corresponding slot */
			slot_ptr = radix_tree_find_slot_ptr(node, chunk);

			if (slot_ptr == NULL)
				goto not_found;

			/* Found! */
			*found = true;
			return *slot_ptr;
		}

		child = radix_tree_find_child(node, key);

		if (child == NULL)
			goto not_found;

		node = child;
		shift -= RADIX_TREE_NODE_FANOUT;
	}

not_found:
	*found = false;
	return (Datum) 0;
}

void
radix_tree_stats(radix_tree *tree)
{
	elog(NOTICE, "nkeys = %lu, height = %u, n4 = %d(%lu), n16 = %d(%lu), n48 = %d(%lu), n256 = %d(%lu)",
		 tree->nkeys,
		 tree->root->shift / RADIX_TREE_NODE_FANOUT,
		 tree->cnt[0], tree->cnt[0] * sizeof(radix_tree_node_4),
		 tree->cnt[1], tree->cnt[1] * sizeof(radix_tree_node_16),
		 tree->cnt[2], tree->cnt[2] * sizeof(radix_tree_node_48),
		 tree->cnt[3], tree->cnt[3] * sizeof(radix_tree_node_256));
	//radix_tree_dump(tree);
}

static void
radix_tree_print_slot(StringInfo buf, uint8 chunk, Datum slot, int idx, bool is_leaf, int level)
{
	char space[128] = {0};

	if (level > 0)
		sprintf(space, "%*c", level * 4, ' ');

	if (is_leaf)
		appendStringInfo(buf, "%s[%d] \"%X\" val(%lu)\n",
						 space,
						 idx,
						 chunk,
						 DatumGetInt64(slot));
	else
		appendStringInfo(buf , "%s[%d] \"%X\" -> ",
						 space,
						 idx,
						 chunk);
}

static void
radix_tree_dump_node(radix_tree_node *node, int level, StringInfo buf)
{
	bool is_leaf = IsLeaf(node);

	appendStringInfo(buf, "[\"%s\" type %d, cnt %u, shift %u, chunk \"%X\"] chunks:\n",
					 IsLeaf(node) ? "LEAF" : "INTR",
					 (node->kind == RADIX_TREE_NODE_KIND_4) ? 4 :
					 (node->kind == RADIX_TREE_NODE_KIND_16) ? 16 :
					 (node->kind == RADIX_TREE_NODE_KIND_48) ? 48 : 256,
					 node->count, node->shift, node->chunk);

	switch (node->kind)
	{
		case RADIX_TREE_NODE_KIND_4:
		{
			radix_tree_node_4 *n4 = (radix_tree_node_4 *) node;

			for (int i = 0; i < n4->n.count; i++)
			{
				radix_tree_print_slot(buf, n4->chunks[i], n4->slots[i], i, is_leaf, level);

				if (!is_leaf)
				{
					StringInfoData buf2;

					initStringInfo(&buf2);
					radix_tree_dump_node((radix_tree_node *) n4->slots[i], level + 1, &buf2);
					appendStringInfo(buf, "%s", buf2.data);
				}
			}
			break;
		}
		case RADIX_TREE_NODE_KIND_16:
		{
			radix_tree_node_16 *n16 = (radix_tree_node_16 *) node;

			for (int i = 0; i < n16->n.count; i++)
			{
				radix_tree_print_slot(buf, n16->chunks[i], n16->slots[i], i, is_leaf, level);

				if (!is_leaf)
				{
					StringInfoData buf2;

					initStringInfo(&buf2);
					radix_tree_dump_node((radix_tree_node *) n16->slots[i], level + 1, &buf2);
					appendStringInfo(buf, "%s", buf2.data);
				}
			}
			break;
		}
		case RADIX_TREE_NODE_KIND_48:
		{
			radix_tree_node_48 *n48 = (radix_tree_node_48 *) node;

			for (int i = 0; i < 256; i++)
			{
				int pos = n48->slot_idxs[i] - 1;

				if (pos < 0)
					continue;

				radix_tree_print_slot(buf, i, n48->slots[pos], i, is_leaf, level);

				if (!is_leaf)
				{
					StringInfoData buf2;

					initStringInfo(&buf2);
					radix_tree_dump_node((radix_tree_node *) n48->slots[pos], level + 1, &buf2);
					appendStringInfo(buf, "%s", buf2.data);
				}
			}
			break;
		}
		case RADIX_TREE_NODE_KIND_256:
		{
			radix_tree_node_256 *n256 = (radix_tree_node_256 *) node;

			for (int i = 0; i < 256; i++)
			{
				if (n256->slots[i] == (Datum) 0)
					continue;

				radix_tree_print_slot(buf, i, n256->slots[i], i, is_leaf, level);

				if (!is_leaf)
				{
					StringInfoData buf2;

					initStringInfo(&buf2);
					radix_tree_dump_node((radix_tree_node *) n256->slots[i], level + 1, &buf2);
					appendStringInfo(buf, "%s", buf2.data);
				}
			}
			break;
		}
			break;
	}
}

void
radix_tree_dump(radix_tree *tree)
{
	StringInfoData buf;

	initStringInfo(&buf);

	elog(NOTICE, "-----------------------------------------------------------");
	elog(NOTICE, "max_val = %lu", tree->max_val);
	radix_tree_dump_node(tree->root, 0, &buf);
	elog(NOTICE, "\n%s", buf.data);
	elog(NOTICE, "-----------------------------------------------------------");
}
