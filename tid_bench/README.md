# bdbench (Benchmark for index bulk-deletion)

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

### 2. Integer set (intset)

Encode TIDs (`ItemPointerData`) to `uint64` and store them to `IntegerSet` (`src/backend/lib/integerset.c`). `IntegerSet` is a tree structure that has `uint64` integers in the leaf node that are encoded by `Simple-8b` encoding.

It has `intset_is_member` for existence check.

### 3. TID bitmap (tbm)

Store dead tuple TIDs to `TIDBitmap` (`src/backend/nodes/tidbitmap.c`).

### 4. Variable TID bitmap (vtbm)

`TIDBitmap` consists of a hash table (`simplehash.h`) whose key is `BlockNumber`. An hash table entry is fixed size and has bits enough to store all possible `OffsetNumber` value. With 8kB blocks, we need 291 bits (`MaxHeapTuplesPerPage`). Therefore, the bitmap is 40 bytes long (an array of five 8 byte-integer elements, 320 bits) and the total entry size is 46 bytes long including other meta information. One concerning thing is that it’s not common in practice that we use up the line pointers. FYI if the table has only one int4 column, about 220 tuples can be stored in a page. In the worst case, even if there is only one dead tuple in the block, we always use 46 bytes whereas we use only 6 bytes with `array`.

**Variable TID bitmap** solves this problem by separating the bitmap space from the hash entry. It consists of hash table having entries per block and a incrementally-growing space commonly used by all block entires to store bitmaps. The hash entry is 12 bytes long and has `BlockNumber` and offset in the space. That way, a block entry needs the space for bitmap as many bytes as it needs to store the bit of the highest offset number in the block. The initial size of the space is 64kB and grows as needed.

### 5. Roaring TID bitmap (rtbm)

`vtbm` still has a room for improvement; we always need bits enough to store the highest offset number in the block. For example, if there is two dead tuples at offset 1 and 150 in the block, we will need at least 150 bits, 19 bytes.

**Roaring TID bitmap** solves this problem by borrowing the idea from Roaring bitmap. rtbm is almost the same as vtbm; it consists of hash table (`simplehash`) and the variable-length space commonly used by every block entry.  But in the variable-length space the data representation varies depending on the cardinarity of the offset number.  We use two (or three) different type of container: `array` and `bitmap`. For example, in the previous example storing two dead tuples at offset 1 and 150, we use the `array` container that has array of two 2-byte integer representing 1 and 150, using 4 bytes in total. On the other hand, if there are concecutive 20 dead tuples from offset 10 to 29 in the block, we use the `bitmap` container that has a bitmap, using 4 bytes (32 bits) instead of storing those 20 2-byte integers as an array that uses 40 bytes.

#### Fallback to `array`

The block entry has a flag indicating the type of container but is 12 bytes. Which means we always need at least 12 bytes to represent at least one TID. Including an additional pointer per block entry used during iteration, it requires at least 20 bytes. Therefore, `array` is still better in some cases, for example, where there are many blocks having only one dead tuple. In those cases, I think we can fallback to the `array` by migrating from `rtbm` to `array` while accepting the slow lookup performance. Temporarily serializing dead tuple TIDs to the disk, freeing the `rtbm`, and loading them to the flat array. Note that there is assum here that doing index vacuum with faster lookup multiple times costs than doing index vacuuming with slow lookup only once. Not sure, it seems to depend on cases.

#### Integrating with `TIDBitmap`

I've considered to integrate either `vtbm` or `rtbm` with `TIDBitmap` but it seems to hard.

`TIDBitmap` supports the concept of `lossy`. When a page becomes lossy, we set the corresponding bit on the `chunk entry` that is different from the `page entry`. Which means the bitmap in the block entry is not stable. It doesn't work with the variable-lentgh space where bitmap (or container in `rtbm` case) is stable.

## Benchmark

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
select bench('array');
```

