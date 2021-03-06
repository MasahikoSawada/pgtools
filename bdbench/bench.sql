\timing on
drop extension bdbench;
create extension bdbench;

select prepare(
1000000, -- max block
10, -- # of dead tuples per page
1, -- dead tuples interval within a page
1 -- page inteval
);

-- Load dead tuples to all data structures.
select 'array', attach_dead_tuples('array');
select 'tbm', attach_dead_tuples('tbm');
select 'intset', attach_dead_tuples('intset');
select 'vtbm', attach_dead_tuples('vtbm');
select 'rtbm', attach_dead_tuples('rtbm');

-- Do benchmark of lazy_tid_reaped.
select 'array bench', bench('array');
select 'intset bench', bench('intset');
select 'rtbm bench', bench('rtbm');
select 'tbm bench', bench('tbm');
select 'vtbm bench', bench('vtbm');

-- Check the memory usage.
select * from pg_backend_memory_contexts where name ~ 'bench' or name = 'TopMemoryContext' order by name;
