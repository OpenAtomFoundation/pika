CLEAN_FILES = # deliberately empty, so we can append below.
CXX=g++
LDFLAGS= -lpthread -lrt
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2 -pipe -fPIC
PROFILING_FLAGS= -pg
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
OPT += -O0 -D__XDEBUG__ -D_GNU_SOURCE $(PROFILING_FLAGS)
endif

#-----------------------------------------------

SRC_DIR=src
VERSION_CC=$(SRC_DIR)/build_version.cc
LIB_SOURCES :=  $(VERSION_CC) \
				$(filter-out $(VERSION_CC), $(wildcard $(SRC_DIR)/*.cc))

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

AM_LINK = $(AM_V_LD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) -I.. $(OPT)

date := $(shell date +%F)
git_sha := $(shell git rev-parse HEAD 2>/dev/null)
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ src/build_version.cc.in
# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
CLEAN_FILES += src/build_version.cc

FORCE: 
src/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then         \
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;    \
	else mv -f $@-t $@; fi

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

# if user didn't config LIBNAME, set the default
ifeq ($(LIBNAME),)
# we should only run slash in production with DEBUG_LEVEL 0
ifeq ($(DEBUG_LEVEL),0)
        LIBNAME=libslash
else
        LIBNAME=libslash_debug
endif
endif
LIBOUTPUT = lib
dummy := $(shell mkdir -p $(LIBOUTPUT))
LIBRARY = $(LIBOUTPUT)/${LIBNAME}.a

TESTS = slash_string_test slash_binlog_test slash_coding_test base_conf_test \
				slash_env_test

EXAMPLES = conf_example cond_lock_example binlog_example mutex_example hash_example

.PHONY: clean dbg static_lib all check example

all: $(LIBRARY)

static_lib: $(LIBRARY)

example: $(LIBRARY) $(EXAMPLES)

check: $(LIBRARY) $(TESTS)
	for t in $(notdir $(TESTS)); do echo "***** Running $$t"; ./$$t || exit 1; done

dbg: $(LIBRARY) $(EXAMPLES)

$(LIBRARY): $(LIBOBJECTS)
	$(AM_V_AR)rm -f $@
	$(AM_V_at)$(AR) $(ARFLAGS) $@ $(LIBOBJECTS)

%.o : %.cc
	$(AM_V_CXX)$(CXX) $(CXXFLAGS) -c $< -o $@

%.d: %.cc
	@set -e; rm -f $@; $(CXX) -MM $(CXXFLAGS) $< > $@.$$$$; \
	sed 's,\($(notdir $*)\)\.o[ :]*,$(SRC_DIR)/\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

ifneq ($(MAKECMDGOALS),clean)
  -include $(LIBOBJECTS:.o=.d)
endif

clean:
	make -C ./examples clean
	rm -f $(TESTS) $(EXAMPLES)
	rm -f $(LIBRARY)
	rm -rf $(CLEAN_FILES)
	rm -rf $(LIBOUTPUT)
	find . -name "*.[oda]*" -exec rm -f {} \;
	find . -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

# tests

TEST_MAIN = tests/test_main.o

slash_string_test: tests/slash_string_test.o $(TEST_MAIN) $(LIBOBJECTS)
	$(AM_LINK)

slash_binlog_test: tests/slash_binlog_test.o $(TEST_MAIN) $(LIBOBJECTS)
	$(AM_LINK)

base_conf_test: tests/base_conf_test.o $(TEST_MAIN) $(LIBOBJECTS)
	$(AM_LINK)

slash_coding_test: tests/slash_coding_test.o $(TEST_MAIN) $(LIBOBJECTS)
	$(AM_LINK)

slash_env_test: tests/slash_env_test.o $(TEST_MAIN) $(LIBOBJECTS)
	$(AM_LINK)

# examples

conf_example: examples/conf_example.o $(LIBOBJECTS)
	$(AM_LINK)

cond_lock_example: examples/cond_lock_example.o $(LIBOBJECTS)
	$(AM_LINK)

binlog_example: examples/binlog_example.o $(LIBOBJECTS)
	$(AM_LINK)

mutex_example: examples/mutex_example.o $(LIBOBJECTS)
	$(AM_LINK)

hash_example: examples/hash_example.o $(LIBOBJECTS)
	$(AM_LINK)
