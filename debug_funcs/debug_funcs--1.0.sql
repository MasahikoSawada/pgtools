/* pgtools */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgtools" to load this file. \quit

CREATE OR REPLACE FUNCTION get_infomask(int)
RETURNS text[] AS
$$
DECLARE
    ret text[];
BEGIN
    if ($1 & X'0001'::integer)::bool then
        ret := array_cat(ret, ARRAY['HASNULLD']);
    end if;
    if ($1 & X'0002'::integer)::bool then
        ret := array_cat(ret, ARRAY['HASEXTERNAL']);
    end if;
    if ($1 & X'0004'::integer)::bool then
        ret := array_cat(ret, ARRAY['HASOID']);
    end if;
    if ($1 & X'0008'::integer)::bool then
        ret := array_cat(ret, ARRAY['HASOID']);
    end if;
    if ($1 & X'0010'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_KEYSHR_LOCK']);
    end if;
    if ($1 & X'0020'::integer)::bool then
        ret := array_cat(ret, ARRAY['COMBOCID']);
    end if;
    if ($1 & X'0040'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_EXCL_LOCK']);
    end if;
    if ($1 & X'0080'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_LOCK_ONLY']);
    end if;
    if ($1 & X'0100'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMIN_COMMITTED']);
    end if;
    if ($1 & X'0200'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMIN_INVALID']);
    end if;
    if ($1 & X'0300'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMIN_FROZEN']);
    end if;
    if ($1 & X'0400'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_COMMITTED']);
    end if;
    if ($1 & X'0800'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_INVALID']);
    end if;
    if ($1 & X'1000'::integer)::bool then
        ret := array_cat(ret, ARRAY['XMAX_IS_MULTI']);
    end if;
    if ($1 & X'2000'::integer)::bool then
        ret := array_cat(ret, ARRAY['UPDATED']);
    end if;
    if ($1 & X'4000'::integer)::bool then
        ret := array_cat(ret, ARRAY['MOVED_OFF']);
    end if;
    if ($1 & X'8000'::integer)::bool then
        ret := array_cat(ret, ARRAY['MOVED_IN']);
    end if;
    RETURN ret;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_infomask2(int)
RETURNS text[] AS
$$
DECLARE
    ret text[];
BEGIN
    if ($1 & X'2000'::integer)::bool then
        ret := array_cat(ret, ARRAY['KEYS_UPDATED']);
    end if;
    if ($1 & X'4000'::integer)::bool then
        ret := array_cat(ret, ARRAY['HOT_UPDATED']);
    end if;
    if ($1 & X'8000'::integer)::bool then
        ret := array_cat(ret, ARRAY['ONLY_TUPLE']);
    end if;
    RETURN ret;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mvcc(
    rel text,
    blkno int,
    ctid OUT text,
    lp_off OUT smallint,
    lp_flag OUT text,
    t_xmin OUT int8,
    t_xmax OUT int8,
    t_infomask OUT text[],
    t_infomask2 OUT text[],
    t_ctid OUT tid
)
RETURNS SETOF RECORD
AS $$
SELECT  '(0,' || lp || ')' AS ctid,
lp_off,
CASE lp_flags
WHEN 0 THEN 'Unused'
WHEN 1 THEN 'Normal'
WHEN 2 THEN 'Redirect to ' || lp_off
WHEN 3 THEN 'Dead'
END,
t_xmin::text::int8 AS xmin,
t_xmax::text::int8 AS xmax,
get_infomask(t_infomask) AS t_infomask,
get_infomask2(t_infomask2) AS t_infomask,
t_ctid
FROM heap_page_items(get_raw_page($1, $2))
order by lp;
$$ LANGUAGE SQL;

CREATE PROCEDURE waste_xid(cnt int) AS $$ DECLARE i int; BEGIN FOR i in 1..cnt LOOP EXECUTE 'SELECT txid_current()'; COMMIT; END LOOP; END; $$ LANGUAGE plpgsql;
