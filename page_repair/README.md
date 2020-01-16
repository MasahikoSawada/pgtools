# page_repair

Individual page repair module using standby's data.


## Installation

Build the source code.

```
$ git clone git@github.com:MasahikoSawada/pgtools.git
$ cd pgtools/page_repair
$ make USE_PGXS=1
# make USE_PGXS=1 install
```

Create `page_repair` extension on PostgreSQL server.

```
$ psql
=# CREATE EXTENSION page_repair
CREATE EXTENSION
```

This extension is tested on only PostgrSQL 12 but it might work on other version PostgreSQL.

# Usage

`page_repair` extension provides the function `pg_repair_page(table regclass, block_number bigint, connstr text)`. Where `table` and `block_number` is the table name and corrupted block number, respectively. `connstr` is the connection string to connect to the standby server. The standby server that can be connected by `connstr` must have the same system identifier as the server on which this function is excuted. This function can be executed on the master server and by superuser. If you want to repair other forks such as freespace map, visibility map you can use the function `pg_repair_page(table regclass, block_number bigint, connstr text, forkname text)` where `forkname` can be `main`, `fsm` `vm`.

`pg_repair_page` acquires `AccessExclusiveLock` on the target relation and might wait for the standby server to catch up to the master server. This function doesn't attempt to repair the page that is marked as *dirty* on the shared buffer because the dirty page will be flushed to the disk and thereby could repair the corrupted page.
