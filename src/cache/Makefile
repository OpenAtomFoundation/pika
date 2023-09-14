CLEAN_FILES = # deliberately empty, so we can append below.
CXX=g++
LDFLAGS= -lpthread -lrt
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2 -pipe -fPIC
PROFILING_FLAGS=-pg
ARFLAGS = rs
OPT=

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2 # compatible with rocksdb
endif

# compile with -O2 if for release
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
ifeq ($(DEBUG_LEVEL),0)
DISABLE_WARNING_AS_ERROR=1
OPT += -O2 -fno-omit-frame-pointer -DNDEBUG
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += -O0 $(PROFILING_FLAGS)
endif

#-----------------------------------------------

SRC_DIR=./src
DEPS_DIR=./deps
VERSION_CC=$(SRC_DIR)/build_version.cc
LIB_SOURCES :=  $(VERSION_CC) \
				$(filter-out $(VERSION_CC), $(wildcard $(SRC_DIR)/*.cc))

ifndef BLACKWIDOW_PATH
  $(warning Warning: missing blackwidow path, using default)
  BLACKWIDOW_PATH=$(DEPS_DIR)/blackwidow
endif

ifndef SLASH_PATH
  $(warning Warning: missing slash path, using default)
  SLASH_PATH=$(DEPS_DIR)/slash
endif

ifndef ROCKSDB_PATH
  $(warning Warning: missing rocksdb path, using default)
  ROCKSDB_PATH=$(DEPS_DIR)/rocksdb
endif

ifndef REDISDB_PATH
  $(warning Warning: missing redisdb path, using default)
  REDISDB_PATH=$(DEPS_DIR)/redisdb
endif

AM_DEFAULT_VERBOSITY = 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $@;
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CXX = $(am__v_CXX_$(V))
am__v_CXX_ = $(am__v_CXX_$(AM_DEFAULT_VERBOSITY))
am__v_CXX_0 = @echo "  CXX     " $@;
am__v_CXX_1 =
LD = $(CXX)
AM_V_LD = $(am__v_LD_$(V))
am__v_LD_ = $(am__v_LD_$(AM_DEFAULT_VERBOSITY))
am__v_LD_0 = @echo "  LD      " $@;
am__v_LD_1 =
AM_V_AR = $(am__v_AR_$(V))
am__v_AR_ = $(am__v_AR_$(AM_DEFAULT_VERBOSITY))
am__v_AR_0 = @echo "  AR      " $@;
am__v_AR_1 =

AM_LINK = $(AM_V_LD)$(CXX) $^ -o $@ $(LDFLAGS)

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

INCLUDE_PATH = -I./include \
							 -I$(SLASH_PATH)/ \
							 -I$(BLACKWIDOW_PATH)/include/ \
							 -I$(ROCKSDB_PATH)/ \
							 -I$(ROCKSDB_PATH)/include \
							 -I$(REDISDB_PATH)

CXXFLAGS += $(WARNING_FLAGS) $(INCLUDE_PATH) $(OPT)

date := $(shell date +%F)
git_sha := $(shell git rev-parse HEAD 2>/dev/null)
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ $(SRC_DIR)/build_version.cc.in
# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
CLEAN_FILES += $(SRC_DIR)/build_version.cc

$(SRC_DIR)/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then         \
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;    \
	else mv -f $@-t $@; fi
FORCE: 

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
# we should only run blackwidow in production with DEBUG_LEVEL 0
LIBNAME=libdory
#ifeq ($(DEBUG_LEVEL),0)
#        LIBNAME=libblackwidow
#else
#        LIBNAME=libblackwidow_debug
#endif
endif
LIBOUTPUT = ./lib
dummy := $(shell mkdir -p $(LIBOUTPUT))
LIBRARY = $(LIBOUTPUT)/${LIBNAME}.a

.PHONY: clean dbg static_lib all example

all: $(LIBRARY)

static_lib: $(LIBRARY)

dbg: $(LIBRARY)

$(LIBRARY): $(LIBOBJECTS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIBOBJECTS)

clean:
	rm -f $(LIBRARY)
	rm -rf $(CLEAN_FILES)
	rm -rf $(LIBOUTPUT)
	find . -name "*.[oda]*" -path "$(SRC_DIR)/*" -exec rm -rf {} \;
	find . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

%.o: %.cc
	$(AM_V_CXX)$(CXX) $(CXXFLAGS) -c $< -o $@
