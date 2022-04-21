# pgtools/bdbench/Makefile

MODULE_big = bdbench
DATA = bdbench--1.0.sql
OBJS = bdbench.o vtbm.o rtbm.o radix.o svtm.o

EXTENSION = bdbench
REGRESS= bdbench

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgtools/bdbench
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
