\timing on
create if not exists tid_bench;

select prepare_index_tuples2();
select prepare_dead_tuples2();

call attach_dead_tuples_to_all();

select 'array', tid_bench('array');
select 'tbm', tid_bench('tbm');
select 'intset', tid_bench('intset');
select 'dtstore', tid_bench('dtstore');
select 'dtstore_r', tid_bench('dtstore_r');
