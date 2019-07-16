# pgtools/col_order/Makefile

MODULE_big = col_order
DATA = col_order--1.0.sql
OBJS = col_order.o

EXTENSION = col_order
REGRESS= col_order

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgtools/col_order
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
