# radix_tree

MODULE_big = radix_tree
DATA = radix_tree--1.0.sql
OBJS = radix_tree.o radix_tree_func.o

EXTENSION = radix_tree
REGRESS= radix_tree

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
