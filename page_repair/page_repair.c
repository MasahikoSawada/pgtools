/* -------------------------------------------------------------------------
 *
 * page_repair.c
 *
 * Repair corrupted page using page on physical standby server.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "access/relation.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_control.h"
#include "catalog/pg_class.h"
#include "fe_utils/connect.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/buf_internals.h"
#include "storage/checksum.h"
#include "storage/lockdefs.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/varlena.h"

#include "libpq-int.h"

PG_MODULE_MAGIC;

#define STANDBY_LSN_CHECK_INTERVAL (5 * 1000L) /* 5 sec */

PGconn *conn = NULL;

PG_FUNCTION_INFO_V1(pg_repair_page);
PG_FUNCTION_INFO_V1(pg_repair_page_fork);
PG_FUNCTION_INFO_V1(get_page);

static void repair_page_internal(Oid oid, BlockNumber blkno, const char *forkname,
								 const char *conninfo);

static void
exec_command(const char *sql)
{
	PGresult *res;

	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		ereport(ERROR,
				(errmsg("error running query (%s) in source server: %s",
						sql, PQresultErrorMessage(res))));

	PQclear(res);
}

static char *
exec_query(const char *sql)
{
    PGresult   *res;
    char       *result;

    res = PQexec(conn, sql);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        ereport(ERROR,
				(errmsg("error running query (%s) in source server: %s",
						sql, PQresultErrorMessage(res))));

    /* sanity check the result set */
    if (PQnfields(res) != 1 || PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
        ereport(ERROR,
				(errmsg("unexpected result set from query")));

    result = pstrdup(PQgetvalue(res, 0, 0));

    PQclear(res);

    return result;
}

static void
check_standby(void)
{
	char	*res;
	uint64	system_identifier;

	if (PQserverVersion(conn) >= 120000)
	{
		system_identifier =
			atol(exec_query("SELECT system_identifier FROM pg_control_system()"));
	}
	else
	{
		ControlFileData *controlfile = (ControlFileData *)
			exec_query("SELECT pg_read_binary_file('global/pg_control')");
		system_identifier = controlfile->system_identifier;
	}

	if (GetSystemIdentifier() != system_identifier)
		ereport(ERROR,
				(errmsg("the server is from different system")));

	res = exec_query("SELECT pg_is_in_recovery()");
	if (strcmp(res, "t") != 0)
		ereport(ERROR,
				(errmsg("the source server must be in recovery mode")));
	pfree(res);
}

static void
connect_standby(const char *conninfo)
{
	if ((conn = PQconnectdb(conninfo)) == NULL)
		ereport(ERROR,
				(errmsg("could not establish connection to server : \"%s\"",
						conninfo)));

	/* disable all types of timeouts */
	exec_command("SET statement_timeout = 0");
	exec_command("SET lock_timeout = 0");
	exec_command("SET idle_in_transaction_session_timeout = 0");
}

static void
check_relation(Relation rel, ForkNumber forknum, BlockNumber blkno)
{
	/* Check that this relation has storage */
	if (rel->rd_rel->relkind == RELKIND_VIEW)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot repair view \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot repair composite type \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot repair foreign table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot repair partitioned table \"%s\"",
						RelationGetRelationName(rel))));
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("cannot repair partitioned index \"%s\"",
						RelationGetRelationName(rel))));

	/*
	 * Reject attempts to read non-local temporary relations; we would be
	 * likely to get wrong data since we have no visibility into the owning
	 * session's local buffers.
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions")));

	if (blkno >= RelationGetNumberOfBlocksInFork(rel, forknum))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("block number %u is out of range for relation \"%s\"",
						blkno, RelationGetRelationName(rel))));

}

static bool
verify_page(BlockNumber blkno, char *page)
{
	uint16		chk_expected;
	uint16		chk_found;

	/* Check if the page is corrupted */
	chk_expected = ((PageHeader) page)->pd_checksum;
	chk_found = pg_checksum_page(page, blkno);

	return chk_expected == chk_found;
}

static void
fetch_page_from_standby(Relation relation, const char *forkname, BlockNumber blkno,
						char *page)
{
	StringInfoData	sql;
	PGresult *res;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT get_page('%s', '%s', %u)",
					 RelationGetRelationName(relation), forkname, blkno);

	res = PQexecParams(conn, sql.data,
					   0, NULL, NULL, NULL, NULL,
					   1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        ereport(ERROR,
				(errmsg("error running query (%s) in source server: %s",
						sql.data, PQresultErrorMessage(res))));

	if (PQgetlength(res, 0, 0) != BLCKSZ)
        ereport(ERROR,
				(errmsg("fetched page length is invalid: expected %d but got %d",
						BLCKSZ, PQgetlength(res, 0, 0))));

	memcpy(page, PQgetvalue(res, 0, 0), BLCKSZ);
	PQclear(res);
}

static void
wait_until_catchup(XLogRecPtr lsn)
{
	int rc;

	for (;;)
	{
		XLogRecPtr standby_lsn;
		char *lsn_str;

		CHECK_FOR_INTERRUPTS();

		lsn_str = exec_query("SELECT pg_last_wal_replay_lsn()");

		standby_lsn =
			DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(lsn_str)));

		/* Standby caught up the master */
		if (lsn <= standby_lsn)
			break;

#if PG_VERSION_NUM >= 100000
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   STANDBY_LSN_CHECK_INTERVAL,
					   PG_WAIT_EXTENSION);
#else
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   STANDBY_LSN_CHECK_INTERVAL);
#endif

		ResetLatch(&MyProc->procLatch);
	}
}

Datum
pg_repair_page_fork(PG_FUNCTION_ARGS)
{
	Oid oid = PG_GETARG_OID(0);
	BlockNumber blkno = PG_GETARG_UINT32(1);
	char *conninfo = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char *forkname = text_to_cstring(PG_GETARG_TEXT_PP(3));

	repair_page_internal(oid, blkno, forkname, conninfo);

	PG_RETURN_BOOL(true);
}

Datum
pg_repair_page(PG_FUNCTION_ARGS)
{
	Oid oid = PG_GETARG_OID(0);
	BlockNumber blkno = PG_GETARG_UINT32(1);
	char *conninfo = text_to_cstring(PG_GETARG_TEXT_PP(2));

	repair_page_internal(oid, blkno, "main", conninfo);

	PG_RETURN_BOOL(true);
}

/* Copied from pageinspect.get_raw_page */
Datum
get_page(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_PP(0);
	text *forkname = PG_GETARG_TEXT_PP(1);
	uint32 blkno = PG_GETARG_UINT32(2);
	ForkNumber forknum = forkname_to_number(text_to_cstring(forkname));
	bytea	   *raw_page;
	RangeVar   *relrv;
	Relation	rel;
	char	   *raw_page_data;
	Buffer		buf;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to use raw page functions"))));

	relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	rel = relation_openrv(relrv, AccessShareLock);

	/* Initialize buffer to copy to */
	raw_page = (bytea *) palloc(BLCKSZ + VARHDRSZ);
	SET_VARSIZE(raw_page, BLCKSZ + VARHDRSZ);
	raw_page_data = VARDATA(raw_page);

	/* Take a verbatim copy of the page */

	buf = ReadBufferExtended(rel, forknum, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	memcpy(raw_page_data, BufferGetPage(buf), BLCKSZ);

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);

	relation_close(rel, AccessShareLock);

	PG_RETURN_BYTEA_P(raw_page);
}

static void
repair_page_internal(Oid oid, BlockNumber blkno, const char *forkname,
					 const char *conninfo)
{
	Relation	relation;
	ForkNumber	forknum = forkname_to_number(forkname);
	BufferTag	tag;
	uint32		taghash;
	LWLock		*partlock;
	int			buf_id;
	char		page[BLCKSZ];
	char		standby_page[BLCKSZ];
	XLogRecPtr	target_lsn;

	if (RecoveryInProgress())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 (errmsg("recovery is in progress"))));

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to execute page reparing function"))));

	if (!DataChecksumsEnabled())
		ereport(ERROR,
				(errmsg("data checksums are not enabled")));

	/* Connect to the standby server and do sanity checks */
	connect_standby(conninfo);
	check_standby();

	/* Open relation and do sanity checks */
	relation = relation_open(oid, AccessExclusiveLock);
	check_relation(relation, forknum, blkno);

	/* Get the current lsn */
	target_lsn = GetXLogWriteRecPtr();

	/* Create buffer tag and compute partition lock ID */
#if PG_VERSION_NUM >= 160000
	InitBufferTag(&tag, &relation->rd_smgr->smgr_rlocator.locator, forknum, blkno);
#else
	INIT_BUFFERTAG(tag, relation->rd_smgr->smgr_rnode.node, forknum, blkno);
#endif
	taghash = BufTableHashCode(&tag);
	partlock = BufMappingPartitionLock(taghash);

	LWLockAcquire(partlock, LW_SHARED);
	buf_id = BufTableLookup(&tag, taghash);
	if (buf_id >= 0)
	{
		BufferDesc *bufHdr;
		uint32	buf_state;

		bufHdr = GetBufferDescriptor(buf_id);
		buf_state = pg_atomic_read_u32(&bufHdr->state);

		/*
		 * The buffer is already is in shard buffer. We do nothing for already-loaded
		 * pages that is dirtied since it will  be flushed to the disk.
		 */
		if ((buf_state & (BM_DIRTY | BM_JUST_DIRTIED)) != 0)
		{
			elog(NOTICE,"skipping page repair of the given page --- page is marked as dirty");
			LWLockRelease(partlock);
			goto cleanup;
		}

		if ((buf_state & BM_VALID) != 0)
		{
			uint32 new_state;
			new_state = LockBufHdr(bufHdr);
			new_state &= ~(BM_VALID);
			UnlockBufHdr(bufHdr, new_state);
		}
	}

	/* Read the page from the disk */
	smgrread(relation->rd_smgr, forknum, blkno, page);
	LWLockRelease(partlock);

	if (verify_page(blkno, page))
	{
		elog(NOTICE, "skipping page repair of the given page --- page is not corrupted");
		goto cleanup;
	}

	/*
	 * The page is corrupted. We wait for the standby's applay LSN  to reach the
	 * current local LSN .
	 */
	wait_until_catchup(target_lsn);

	/* Get the target page from the standby server */
	fetch_page_from_standby(relation, forkname, blkno, standby_page);

	/* Verify the page got from the standby server */
	if (!verify_page(blkno, standby_page))
		ereport(ERROR,
				(errmsg("page on standby is also corrupted")));

	/* Overwrite the corrupted page */
	smgrwrite(relation->rd_smgr, forknum, blkno, standby_page, 1);
	smgrimmedsync(relation->rd_smgr, forknum);

cleanup:
	relation_close(relation, NoLock);
	PQfinish(conn);
}
