/* radix_tree */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION radix_tree" to load this file. \quit

CREATE FUNCTION run_test()
RETURNS VOID
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
