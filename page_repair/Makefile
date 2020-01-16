# pgtools/page_repair/Makefile

MODULE_big = page_repair
PGFILEDESC = "page_repair - repair corrupted page"

EXTENSION = page_repair
DATA = page_repair--1.0.sql
OBJS = $(WIN32RES) page_repair.o

TAP_TESTS = 1

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK = $(libpq_pgport)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pgtools/page_repair
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
