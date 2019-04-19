# pgtools/debug_funcs/Makefile

MODULE_big = debug_funcs
DATA = debug_funcs--1.0.sql

EXTENSION = debug_funcs

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgtools/debug_funcs
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
