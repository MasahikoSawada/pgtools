# pgtools/tid_benchbenc/Makefile

MODULE_big = tid_bench
DATA = tid_bench--1.0.sql
OBJS = tid_bench.o lvdeadtuple.o

EXTENSION = tid_bench
REGRESS= tid_bench

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgtools/tid_bench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
