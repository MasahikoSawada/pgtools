\timing on
drop extension bdbench;
create extension bdbench;

--select prepare(
--1000000, -- max block
--10, -- # of dead tuples per page
--1, -- dead tuples interval within  a page
--1, -- # of consecutive pages having dead tuples
--20 -- page interval
--);

--select prepare(
--1000000, -- max block
--10, -- # of dead tuples per page
--1, -- dead tuples interval within  a page
--1, -- # of consecutive pages having dead tuples
--1 -- page interval
--);

select prepare(
1000000, -- max block
20, -- # of dead tuples per page
10, -- dead tuples interval within a page
1,
1 -- page inteval
);

-- Load dead tuples to all data structures.
--select 'array', attach_dead_tuples('array');
select 'intset', attach_dead_tuples('intset');
select 'rtbm', attach_dead_tuples('rtbm');
select 'tbm', attach_dead_tuples('tbm');
--select 'vtbm', attach_dead_tuples('vtbm');
select 'radix', attach_dead_tuples('radix');
--select 'svtm', attach_dead_tuples('svtm');
select 'radix_tree', attach_dead_tuples('radix_tree');

-- Do benchmark of lazy_tid_reaped.
--select 'array bench', bench('array');
select 'intset bench', bench('intset');
select 'rtbm bench', bench('rtbm');
select 'tbm bench', bench('tbm');
--select 'vtbm bench', bench('vtbm');
select 'radix', bench('radix');
--select 'svtm', bench('svtm');
select 'radix_tree', bench('radix_tree');


-- Check the memory usage.
with recursive a (name, hist) as (
select name, array[name] from pg_backend_memory_contexts where name = 'radix_tree bench'
union all
select b.name,  hist || b.name from a, pg_backend_memory_contexts b where b.parent = a.name)
select 'radix_tree', pg_size_pretty(sum(total_bytes)) as total_bytes, pg_size_pretty(sum(used_bytes)) as used_bytes from pg_backend_memory_contexts b where name in (select name from a);

with recursive a (name, hist) as (
select name, array[name] from pg_backend_memory_contexts where name = 'radix bench'
union all
select b.name,  hist || b.name from a, pg_backend_memory_contexts b where b.parent = a.name)
select 'radix', pg_size_pretty(sum(total_bytes)) as total_bytes, pg_size_pretty(sum(used_bytes)) as used_bytes from pg_backend_memory_contexts b where name in (select name from a);

with recursive a (name, hist) as (
select name, array[name] from pg_backend_memory_contexts where name = 'intset bench'
union all
select b.name,  hist || b.name from a, pg_backend_memory_contexts b where b.parent = a.name)
select 'intset', pg_size_pretty(sum(total_bytes)) as total_bytes, pg_size_pretty(sum(used_bytes)) as used_bytes from pg_backend_memory_contexts b where name in (select name from a);

with recursive a (name, hist) as (
select name, array[name] from pg_backend_memory_contexts where name = 'rtbm bench'
union all
select b.name,  hist || b.name from a, pg_backend_memory_contexts b where b.parent = a.name)
select 'rtbm_tree', pg_size_pretty(sum(total_bytes)) as total_bytes, pg_size_pretty(sum(used_bytes)) as used_bytes from pg_backend_memory_contexts b where name in (select name from a);

with recursive a (name, hist) as (
select name, array[name] from pg_backend_memory_contexts where name = 'tbm bench'
union all
select b.name,  hist || b.name from a, pg_backend_memory_contexts b where b.parent = a.name)
select 'tbm_tree', pg_size_pretty(sum(total_bytes)) as total_bytes, pg_size_pretty(sum(used_bytes)) as used_bytes from pg_backend_memory_contexts b where name in (select name from a);
