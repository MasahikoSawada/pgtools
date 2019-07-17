/* -------------------------------------------------------------------------
 *
 * col_order.c
 *
 * An advisory tool for suitable column definition order.
 *
 * -------------------------------------------------------------------------
 */

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

PG_MODULE_MAGIC;

Size minSize = INT_MAX;
List *minOrder = NIL;

bool col_order_debug_enabled = false;

PG_FUNCTION_INFO_V1(compute_col_order);

void _PG_init(void);
static void compute_col_order_recurse(List *target, List *remain);
static Oid *get_type_oid_contents(ArrayType *array, int *numitems);
static Size compute_data_size(List *types);

void
_PG_init(void)
{
	DefineCustomBoolVariable("col_order.debug_enabled",
							 "enable debug print",
							 NULL,
							 &col_order_debug_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);
}

static void
dump_order(List *l, Size size, char *msg)
{
	ListCell *cell;
	StringInfoData buf;

	initStringInfo(&buf);
	foreach(cell, l)
	{
		Form_pg_type type = (Form_pg_type) lfirst(cell);
		appendStringInfo(&buf, "%u ", type->oid);
	}

	elog(NOTICE, "%s: %lu %s (minSize %lu)",
		 buf.data, size,
		 msg == NULL ? "" : msg,
		 minSize);
	pfree(buf.data);
}

/* Return list of data type oids from 'array */
static Oid *
get_type_oid_contents(ArrayType *array, int *numitems)
{
	int		ndim = ARR_NDIM(array);
	int		*dims = ARR_DIMS(array);
	int		nitems;
	int16	typlen;
	bool	typbyval;
	char	typalign;
	Oid		*values;
	bits8	*bitmap;
	int		bitmask;
	char	*ptr;
	int		i;

	*numitems = nitems = ArrayGetNItems(ndim, dims);

	get_typlenbyvalalign(ARR_ELEMTYPE(array),
						 &typlen, &typbyval, &typalign);

	values = (Oid *) palloc(nitems * sizeof(Oid));

	ptr = ARR_DATA_PTR(array);
	bitmap = ARR_NULLBITMAP(array);
	bitmask = 1;

	for (i = 0; i < nitems; i++)
	{
		if (bitmap && (*bitmap & bitmask) == 0)
			elog(ERROR, "could not specify NULL at %d", i + 1);
		else
		{
			values[i] = *(Oid*) ptr;
			ptr = att_addlength_pointer(ptr, typlen, ptr);
			ptr = (char *) att_align_nominal(ptr, typalign);
		}

		if (bitmap)
		{
			bitmask <<= 1;
			if (bitmask == 0x100)
			{
				bitmap++;
				bitmask = 1;
			}
		}
	}

	return values;
}

/*
 * Compute column definition order with minimum length.
 */
static void
compute_col_order_recurse(List *target, List *remain)
{
	ListCell *lc;
	Size size;
	int n = 0;

	/* compute col size so far */
	size = compute_data_size(target);

	/* computed size of all columns */
	if (remain == NIL)
	{
		if (col_order_debug_enabled)
		{
			if (size < minSize)
				dump_order(target, size, "(selected)");
			else
				dump_order(target, size, "(not selected)");
		}

		/* update minimun size and order */
		if (size < minSize)
		{
			minSize = size;
			minOrder = list_copy(target);
		}
		return;
	}

	/* quick exit, if the size so far is greater than minimun size */
	if (minSize < size)
	{
		if (col_order_debug_enabled)
			dump_order(target, size, "(skipped)");

		return;
	}

	if (col_order_debug_enabled)
		dump_order(target, size, NULL);

	foreach(lc, remain)
	{
		List *t = NIL, *r = NIL;
		Form_pg_type type = (Form_pg_type) lfirst(lc);
		Form_pg_type del;

		t = list_copy(target);
		r = list_copy(remain);

		/* copy a type from remain... */
		t = lappend(t, type);

		/* and then delete it from remain */
		del = (Form_pg_type) list_nth(r, n);
		r = list_delete_ptr(r, (void *) del);

		compute_col_order_recurse(t, r);
		list_free(t);
		list_free(r);
		n++;
	}
}

/*
 * Brrowed heap_compute_data_size() in heaptuple.c
 */
static Size
compute_data_size(List *types)
{
	Size data_length = 0;
	ListCell *lc;

	foreach (lc, types)
	{
		Form_pg_type type = (Form_pg_type) lfirst(lc);
		data_length = att_align_nominal(data_length, type->typalign);
		if (type->typlen == -1)
		{
			/* Assume varlena size is fixed size, 100 */
			data_length += 100;
		}
		else
			data_length += type->typlen;
	}
	return MAXALIGN(data_length);
}

Datum
compute_col_order(PG_FUNCTION_ARGS)
{
	ArrayType *type_array = PG_GETARG_ARRAYTYPE_P(0);
	TupleDesc	tupdesc;
	HeapTuple	resultTuple;
	int		ntypes;
	Oid		*oids;
	Datum	*res_oids;
	List	*types = NIL;	/* list of Form_pg_attribute */
	ListCell *lc;
	int		i;
	Datum	values[2];
	bool	nulls[2];

	/* Initialize */
	minSize = INT_MAX;
	minOrder = NIL;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a rowtype");

	oids = get_type_oid_contents(type_array, &ntypes);

	/* get list of Form_pg_type */
	for (int i = 0; i < ntypes; i++)
	{
		Form_pg_type	type = (Form_pg_type) palloc(sizeof(FormData_pg_type));
		Form_pg_type	t;
		HeapTuple		htup;

		htup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(oids[i]));
		if (!HeapTupleIsValid(htup))
			elog(ERROR, "cache lookup failed for type %u", oids[i]);

		/* Get pg_type tuple */
		t = (Form_pg_type) GETSTRUCT(htup);

		/* Copy */
		memcpy(type, t, sizeof(FormData_pg_type));

		/* Append it to the list */
		types = lappend(types, type);

		ReleaseSysCache(htup);
	}

	/* compute the smallest column definition order */
	compute_col_order_recurse(NIL, types);

	/* make oid list in form of Datum for arrray construction */
	i = 0;
	res_oids = palloc(sizeof(Datum) * ntypes);
	foreach(lc, minOrder)
	{
		Form_pg_type t = (Form_pg_type) lfirst(lc);
		res_oids[i++] = UInt32GetDatum(t->oid);
	}

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int64GetDatum(minSize);
	values[1] = PointerGetDatum(construct_array(res_oids, i,
												REGTYPEOID, sizeof(Oid), true, 'i'));

	/* built result heap tuple and return */
	resultTuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(resultTuple);
}
