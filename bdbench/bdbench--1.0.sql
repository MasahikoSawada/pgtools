/* bdbench */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION bdbench" to load this file. \quit

-- default values simulate:
-- * table has 1,000,000,000 tuples
-- * table size is about 100GB (11,000,000 blocks)
-- * dead tuples are 10% of table and distibuted uniformely

CREATE FUNCTION prepare_index_tuples(
nitems bigint default 1000000000, -- index tuples
minblk int default 0,
maxblk int default 1100000,
maxoff int default 100)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION prepare_dead_tuples(
nitems bigint default 200000000, -- dead tuples
minblk int default 0,
maxblk int default 1100000,
maxoff int default 100)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION prepare_index_tuples2(
ntuples bigint default 1000000000,
tuple_size int default 100)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION prepare_dead_tuples2(
ntuples bigint default 1000000000,
tuple_size int default 100,
dt_ratio float8 default 0.2)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION prepare(
maxblk bigint,
dt_per_page int,
dt_interval_in_page int,
dt_interval int)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION prepare_dead_tuples2_packed(
ntuples bigint default 1000000000,
tuple_size int default 100,
dt_ratio float8 default 0.2)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION attach_dead_tuples(
mode text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE PROCEDURE attach_dead_tuples_to_all()
BEGIN ATOMIC
SELECT attach_dead_tuples('array');
SELECT attach_dead_tuples('tbm');
SELECT attach_dead_tuples('intset');
SELECT attach_dead_tuples('dtstore');
SELECT attach_dead_tuples('dtstore_r');
END;

CREATE FUNCTION bench(
mode text default 'array')
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

--CREATE FUNCTION itereate_bench(
--mode text default 'array')
--RETURNS text
--AS 'MODULE_PATHNAME'
--LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION test_generate_tid(
nitems bigint,
minblk int,
maxblk int,
maxoff int)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

--CREATE FUNCTION tbm_test()
--RETURNS text
--AS 'MODULE_PATHNAME'
--LANGUAGE C STRICT VOLATILE;

--CREATE FUNCTION dtstore_test()
--RETURNS text
--AS 'MODULE_PATHNAME'
--LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION rtbm_test()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION radix_run_tests()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
