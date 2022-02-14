#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/ps_status.h"
#include "utils/memutils.h"

#include "radix_tree.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(run_test);

static void test_insert_search(radix_tree *rt, uint64 key, Datum val, int i);

static void
test_insert_search(radix_tree *rt, uint64 key, Datum val, int i)
{
	bool found;
	Datum ret;
	char buf[256] = {0};

	sprintf(buf, "[%d] test key %016lX val %d ... ",
			i, key, DatumGetInt32(val));

	radix_tree_insert(rt, key, val);
	ret = radix_tree_search(rt, key, &found);

	if (found)
		sprintf(buf + strlen(buf) - 1, "ok (ret=%d)",
				DatumGetInt32(ret));
	else
		sprintf(buf + strlen(buf) - 1, "ng (ret=%d)",
				DatumGetInt32(ret));

	elog(NOTICE, "%s", buf);

	Assert(found);
	Assert(DatumGetInt32(val) == DatumGetInt32(ret));
}

static uint64
rand_uint64(void) {
	uint64 r = 0;
	for (int i=0; i<64; i += 15 /*30*/)
	{
		r = r*((uint64)RAND_MAX + 1) + rand();
	}
	return r;
}

static void
test_mask(uint64 mask, int n)
{
	radix_tree *tree = radix_tree_create(CurrentMemoryContext);

	for (int i = 0; i < n; i++)
	{
		uint64 key = rand_uint64();

		key &= mask;
		test_insert_search(tree, key, Int32GetDatum(100), i);
	}

	radix_tree_destroy(tree);
}

static void
test_sequence(int n)
{
	radix_tree *tree = radix_tree_create(CurrentMemoryContext);

	for (uint64 i = 0; i < n; i++)
		test_insert_search(tree, i, Int32GetDatum(100), i);
}

static void
test_set(uint64 *keys, int nkeys)
{
	radix_tree *tree = radix_tree_create(CurrentMemoryContext);

	elog(NOTICE, "insert and search test ...");
	for (int i = 0; i < nkeys; i++)
		test_insert_search(tree, keys[i], Int32GetDatum(100), i);

	radix_tree_dump(tree);

	elog(NOTICE, "search test ...");
	for (int i = 0; i < nkeys; i++)
	{
		char buf[256] = {0};
		bool found;
		Datum ret;

		ret = radix_tree_search(tree, keys[i], &found);

		sprintf(buf, "[%d] test key %016lX ... ",
				i, keys[i]);

		if (found)
			sprintf(buf + strlen(buf) - 1, "ok (ret=%d)",
					DatumGetInt32(ret));
		else
			sprintf(buf + strlen(buf) - 1, "ng (ret=%d)",
					DatumGetInt32(ret));

		elog(NOTICE, "%s", buf);

		Assert(found);
		Assert(DatumGetInt32(100) == DatumGetInt32(ret));
	}

	radix_tree_destroy(tree);
}

Datum
run_test(PG_FUNCTION_ARGS)
{
	radix_tree *tree;

	tree = radix_tree_create(CurrentMemoryContext);

	/*
	test_mask(0xFFFFFFFFFFFFFF00, 1000000);
	test_mask(0xFFFFFFFFFFFF00FF, 1000000);
	test_mask(0xFFFFFFFFFF00FFFF, 1000000);
	test_mask(0xFFFFFFFF00FFFFFF, 1000000);
	test_mask(0xFFFFFF00FFFFFFFF, 1000000);
	test_mask(0xFFFF00FFFFFFFFFF, 1000000);
	test_mask(0xFF00FFFFFFFFFFFF, 1000000);
	test_mask(0x00FFFFFFFFFFFFFF, 1000000);
	test_mask(0xFFFFFFFFFFFF0000, 1000000);
	test_mask(0xFFFFFFFFFF000000, 1000000);
	test_mask(0xFFFFFFFF00000000, 1000000);
	test_mask(0xFFFFFF0000000000, 1000000);
	test_mask(0xFFFFFF00000000FF, 1000000);
	*/

	//test_sequence(10000000);

	uint64 keys[] = {
		0x00000000000000AA,
		0x0000000000AA00AA,
		0x000000AA000000AA,
		0x000000AABB0000AA,
		0x000000AACC00BBAA,
		0xAA0000AACC00BBAA,
		0xBB0000AACC00BBAA,
		0x00CC00AACC00BBAA
	};

	test_set(keys, 8);

	/*
	radix_tree_insert(tree,
					  0x000000000000AAAA,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x000000000000AABB,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x000000000000BBAA,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x000000000000BBBB,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x0000000000CCBBBB,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x0000000000DDAABB,
					  Int32GetDatum(100));
	radix_tree_insert(tree,
					  0x0000000000112233,
					  Int32GetDatum(100));
	*/

	//radix_tree_dump(tree);

	PG_RETURN_VOID();
}
