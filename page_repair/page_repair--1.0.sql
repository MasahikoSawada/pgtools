/* page_repair */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION page_repair" to load this file. \quit

CREATE FUNCTION pg_repair_page(regclass, bigint, text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_repair_page'
LANGUAGE C STRICT PARALLEL UNSAFE;

CREATE FUNCTION pg_repair_page(regclass, bigint, text, text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pg_repair_page_fork'
LANGUAGE C STRICT PARALLEL UNSAFE;
