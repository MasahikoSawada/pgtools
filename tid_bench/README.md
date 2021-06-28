# bdbench (Micro benchmark for index bulk-deletion)

Lazy vacuum's index bulk-deletion operation is essentially an existence check. For each index tuples, we check if there is its (heap) TID in the dead tuple TID collected during heap scan. The memory space is limited by maintenance_work_mem, 64MB by default. If we collect dead tuple more than the limit, we suspend the heap scan, invoke both index vacuuming and heap vacuuming, and then resume the heap scan. Since index bulk-deletion is usually implemented as a whole index scan it would be advisable to avoid doing it more than once.

There are two important factors here: memory usage and lookup performance. Both are related to the bulk-deletion performance. The latter is obvious while the former contributes to the number of index vacuuming in a lazy vacuum.

As of the current implementation, we use a simple flat array to store dead tuple TIDs and lookup a TID by `bsearch()`. There are known limitations and problems:

* Cannot allocate more than 1GB.
  * There was a discussion to eliminate this limitation by using `MemoryContextAllocHuge()` but was rejected because of the following two points.
* Allocate the whole memory space at once.
* Slow performance (O(logN)).

This tool is aimed to do micro benchmark for index bulk-deletion with various types of data structure.

## Installation

```bash
$ make USE_PGXS=1
# make install
$ psql
=# CREATE EXTENSION bdbench;
```

## Methods of storing dead tuple TIDs

| Data Structure      | Memory Allocation    | Lookup Time Complexity | Memory Space                            |
|---------------------|----------------------|------------------------|-----------------------------------------|
| Flat array          | Batch at the startup | O(logN)                | 4byte per TID                           |
| Integer set         | Incremental          | O(logN)                | Depends on values (Simple-8B encoding)  |
| TIB bitmap          | Incremental          | O(1)                   | 48 byte per block                       |
| Variable TID bitmap | Incremantal          | O(1)                   | Depends on values (20 bytes at minimum) |
| Roaring TID bitmap  | Incremental          | O(1)                   | Depends on values (20 bytes at minimum) |

### 1. Flat array (array)

Dead tuple TIDs are stored into the simple flat array.

Minimum data stru

### 2. Integer set (intset)

### 3. TID bitmap (tbm)

### 4. Variable TID bitmap (vtbm)

### 5. Roaring TID bitmap (rtbm)

## Micro Benchmark

1. Generate index tuple TIDs and dead tuple TIDs

```sql
select prepare(x, x, x, x);
```

All TIDs are allocated in `TopMemoryContext`, lasting until the proc exit.

2. Load dead tuple TIDs to the specific method

```sql
select attach_dead_tuples('array');
```

The argument can be one of the supported methods.

3. Do benchmark

```sql
\timing on
select bench();
```

