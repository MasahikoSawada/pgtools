# col_order

A function to compute the minimum-size column definition order.

# Installation

Build the source code.

```
$ git clone git@github.com:MasahikoSawada/pgtools.git
$ cd pgtools/col_order
$ make USE_PGXS=1
# make USE_PGXS=1 install
```

Create `col_order` extension on PostgreSQL server.

```
$ psql
=# CREATE EXTENSION col_order
CREATE EXTENSION
```

This extension is tested on only PostgrSQL 12 but it might work on other version PostgreSQL.

# Usage

`col_order` extension provides one SQL function `compute_col_order(regtype[])`.

`compute_col_order` function returns the column definition order that is minimum foot print.

```
=# SELECT * FROM compute_col_order(ARRAY['bigint'::regtype, 'int', 'timestamptz', 'text']);

min_size |                    min_order
----------+--------------------------------------------------
      120 | {bigint,integer,text,"timestamp with time zone"}
(1 row)
```

To compute the minmum size column definition order `compute_col_order` uses DP (dynamic programing).

# Debugging

`compute_col_order` emits condidates of column order when `col_order.debug_enabled` is true.

```
=# SET col_order.debug_enabled TO true;
=# SELECT * FROM compute_col_order(ARRAY['bigint'::regtype, 'timestamptz', 'text']);
NOTICE:  : 0 (intermediate) (threshold 2147483647)
NOTICE:  20 : 8 (intermediate) (threshold 2147483647)
NOTICE:  20 1184 : 16 (intermediate) (threshold 2147483647)
NOTICE:  20 1184 25 : 120 (selected) (threshold 2147483647)
NOTICE:  20 25 : 112 (intermediate) (threshold 120)
NOTICE:  20 25 1184 : 120 (not selected) (threshold 120)
NOTICE:  1184 : 8 (intermediate) (threshold 120)
NOTICE:  1184 20 : 16 (intermediate) (threshold 120)
NOTICE:  1184 20 25 : 120 (not selected) (threshold 120)
NOTICE:  1184 25 : 112 (intermediate) (threshold 120)
NOTICE:  1184 25 20 : 120 (not selected) (threshold 120)
NOTICE:  25 : 104 (intermediate) (threshold 120)
NOTICE:  25 20 : 112 (intermediate) (threshold 120)
NOTICE:  25 20 1184 : 120 (not selected) (threshold 120)
NOTICE:  25 1184 : 112 (intermediate) (threshold 120)
NOTICE:  25 1184 20 : 120 (not selected) (threshold 120)
 min_size |                min_order
----------+------------------------------------------
      120 | {bigint,"timestamp with time zone",text}
(1 row)
```
