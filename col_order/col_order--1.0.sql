/* col_order */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION col_order" to load this file. \quit

CREATE FUNCTION compute_col_order(
IN types regtype[],
OUT min_size bigint,
OUT min_order regtype[])
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;
