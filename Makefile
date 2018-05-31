CLEAN_FILES = # deliberately empty, so we can append below.
CXX=g++
PLATFORM_LDFLAGS= -lpthread -lrt -lunwind
PLATFORM_CXXFLAGS= -std=c++11 -fno-builtin-memcmp -msse -msse4.2 
PROFILING_FLAGS=-pg
OPT=
LDFLAGS += -Wl,-rpath=$(RPATH)

# DEBUG_LEVEL can have two values:
# * DEBUG_LEVEL=2; this is the ultimate debug mode. It will compile pika
# without any optimizations. To compile with level 2, issue `make dbg`
# * DEBUG_LEVEL=0; this is the debug level we use for release. If you're
# running pika in production you most definitely want to compile pika
# with debug level 0. To compile with level 0, run `make`,

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2
endif

ifneq ($(DISABLE_UPDATE_SB), 1)
$(info updating submodule)
dummy := $(shell (git submodule init && git submodule update))
endif

# compile with -O2 if debug level is not 2
ifneq ($(DEBUG_LEVEL), 2)
OPT += -O2 -fno-omit-frame-pointer
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
OPT += -DNDEBUG
DISABLE_WARNING_AS_ERROR=1
# Skip for archs that don't support -momit-leaf-frame-pointer
ifeq (,$(shell $(CXX) -fsyntax-only -momit-leaf-frame-pointer -xc /dev/null 2>&1))
OPT += -momit-leaf-frame-pointer
endif
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += $(PROFILING_FLAGS)
DEBUG_SUFFIX = "_debug"
endif

# Link tcmalloc if exist
dummy := $(shell ("$(CURDIR)/detect_environment" "$(CURDIR)/make_config.mk"))
include make_config.mk
CLEAN_FILES += $(CURDIR)/make_config.mk
PLATFORM_LDFLAGS += $(TCMALLOC_LDFLAGS)
PLATFORM_LDFLAGS += $(ROCKSDB_LDFLAGS)
PLATFORM_CXXFLAGS += $(TCMALLOC_EXTENSION_FLAGS)

# ----------------------------------------------
OUTPUT = $(CURDIR)/output
THIRD_PATH = $(CURDIR)/third
SRC_PATH = $(CURDIR)/src

# ----------------Dependences-------------------

ifndef SLASH_PATH
SLASH_PATH = $(THIRD_PATH)/slash
endif
SLASH = $(SLASH_PATH)/slash/lib/libslash$(DEBUG_SUFFIX).a

ifndef PINK_PATH
PINK_PATH = $(THIRD_PATH)/pink
endif
PINK = $(PINK_PATH)/pink/lib/libpink$(DEBUG_SUFFIX).a

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(THIRD_PATH)/rocksdb
endif
ifeq ($(USE_DYNAMIC_ROCKSDB),1)
ROCKSDB = $(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).so
else
ROCKSDB = $(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a
endif

ifndef NEMODB_PATH
NEMODB_PATH = $(THIRD_PATH)/nemo-rocksdb
endif
NEMODB = $(NEMODB_PATH)/lib/libnemodb$(DEBUG_SUFFIX).a

ifndef NEMO_PATH
NEMO_PATH = $(THIRD_PATH)/nemo
endif
NEMO = $(NEMO_PATH)/lib/libnemo$(DEBUG_SUFFIX).a

ifndef GLOG_PATH
GLOG_PATH = $(THIRD_PATH)/glog
endif

ifndef GFLAGS_PATH
GFLAGS_PATH = $(THIRD_PATH)/gflags
endif

ifndef GTEST_PATH
GTEST_PATH = $(THIRD_PATH)/googletest
endif

ifeq ($(360), 1)
GLOG := -lglog
endif

INCLUDE_PATH = -I. \
							 -I$(GLOG_PATH)/build/src \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH) \
							 -I$(NEMO_PATH)/include \
							 -I$(NEMODB_PATH)/include \
							 -I$(ROCKSDB_PATH) \
							 -I$(ROCKSDB_PATH)/include

ifeq ($(360),1)
INCLUDE_PATH += -I$(GLOG_PATH)/src \
							 -I$(GFLAGS_PATJ)/build/include \
							 -I$(GTEST_PATH)/googletest/include
endif

LIB_PATH = -L./ \
					 -L$(GFLAGS_PATH)/build/lib \
					 -L$(SLASH_PATH)/slash/lib \
					 -L$(PINK_PATH)/pink/lib \
					 -L$(NEMO_PATH)/lib \
					 -L$(NEMODB_PATH)/lib \
					 -L$(ROCKSDB_PATH)

ifeq ($(360),1)
LIB_PATH += -L$(GLOG_PATH)/build/.libs \
					 -L$(GFLAGS_PATH)/build/lib \
					 -L$(GTEST_PATH)/build/googlemock/gtest
endif

LDFLAGS += $(LIB_PATH) \
			 		 -lpink$(DEBUG_SUFFIX) \
			 		 -lslash$(DEBUG_SUFFIX) \
					 -lglog \
					 -lgflags
ifneq ($(USE_DYNAMIC_ROCKSDB),1)
LDFLAGS += -llz4 -lzstd -lbz2 -lsnappy -lz
endif

# ---------------End Dependences----------------

VERSION_CC=$(SRC_PATH)/build_version.cc
LIB_SOURCES :=  $(VERSION_CC) \
				$(filter-out $(VERSION_CC), $(wildcard $(SRC_PATH)/*.cc))


#-----------------------------------------------

AM_DEFAULT_VERBOSITY = 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $(notdir $@);
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $(notdir $@);
am__v_CC_1 =
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $(notdir $@);
am__v_CCLD_1 =

AM_LINK = $(AM_V_CCLD)$(CXX) $^ $(EXEC_LDFLAGS) -o $@ $(LDFLAGS)

CXXFLAGS += -g

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  							-Wno-unused-parameter -Woverloaded-virtual \
							-Wnon-virtual-dtor -Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(INCLUDE_PATH) $(PLATFORM_CXXFLAGS) $(OPT)

LDFLAGS += $(PLATFORM_LDFLAGS)

date := $(shell date +%F)
git_sha := $(shell git rev-parse HEAD 2>/dev/null)
gen_build_version = sed -e s/@@GIT_SHA@@/$(git_sha)/ -e s/@@GIT_DATE_TIME@@/$(date)/ src/build_version.cc.in
# Record the version of the source that we are compiling.
# We keep a record of the git revision in this file.  It is then built
# as a regular source file as part of the compilation process.
# One can run "strings executable_filename | grep _build_" to find
# the version of the source that we used to build the executable file.
CLEAN_FILES += $(SRC_PATH)/build_version.cc

$(SRC_PATH)/build_version.cc: FORCE
	$(AM_V_GEN)rm -f $@-t
	$(AM_V_at)$(gen_build_version) > $@-t
	$(AM_V_at)if test -f $@; then         \
	  cmp -s $@-t $@ && rm -f $@-t || mv -f $@-t $@;    \
	else mv -f $@-t $@; fi
FORCE: 

LIBOBJECTS = $(LIB_SOURCES:.cc=.o)

# if user didn't config LIBNAME, set the default
ifeq ($(BINNAME),)
# we should only run pika in production with DEBUG_LEVEL 0
BINNAME=pika$(DEBUG_SUFFIX)
endif
BINARY = ${BINNAME}

.PHONY: distclean clean dbg all

%.o: %.cc
	  $(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

all: $(BINARY)

dbg: $(BINARY)

$(BINARY): $(LIBOBJECTS) $(SLASH) $(PINK) $(NEMO) $(NEMODB) $(ROCKSDB) $(GLOG)
	$(AM_V_at)rm -f $@
	$(AM_V_at)$(AM_LINK)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)mkdir -p $(OUTPUT)/bin
	$(AM_V_at)mv $@ $(OUTPUT)/bin
	$(AM_V_at)cp -r $(CURDIR)/conf $(OUTPUT)
ifeq ($(USE_DYNAMIC_ROCKSDB),1)
	$(AM_V_at)cp -d $(ROCKSDB)* $(OUTPUT)/bin
endif


$(SLASH):
	$(AM_V_at)make -C $(SLASH_PATH)/slash/ DEBUG_LEVEL=$(DEBUG_LEVEL)

$(PINK):
	$(AM_V_at)make -C $(PINK_PATH)/pink/ DEBUG_LEVEL=$(DEBUG_LEVEL) NO_PB=1 SLASH_PATH=$(SLASH_PATH)

$(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a:
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ static_lib DISABLE_JEMALLOC=1 DEBUG_LEVEL=$(DEBUG_LEVEL)

$(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).so:
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ shared_lib DISABLE_JEMALLOC=1 DEBUG_LEVEL=$(DEBUG_LEVEL)

$(NEMODB):
	$(AM_V_at)make -C $(NEMODB_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) DEBUG_LEVEL=$(DEBUG_LEVEL)

$(NEMO):
	$(AM_V_at)make -C $(NEMO_PATH) NEMODB_PATH=$(NEMODB_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) DEBUG_LEVEL=$(DEBUG_LEVEL)

$(GLOG): FORCE
	cd $(GFLAGS_PATH); mkdir -p build; cd build; if [ ! -e ./Makefile ]; then cmake -DCMAKE_CXX_FLAGS=-fPIC ..; fi; make -j $(PROCESSOR_NUMS); echo 'build' > $(GFLAGS_PATH)/.gitignore;
	cd $(GTEST_PATH); mkdir -p build; cd build; if [ ! -e ./Makefile ]; then cmake -DCMAKE_CXX_FLAGS=-fPIC ..; fi; make -j $(PROCESSOR_NUMS); echo 'build' > $(GTEST_PATH)/.gitignore;
	cd $(GLOG_PATH); mkdir -p build; cd build; if [ ! -e ./Makefile ]; then ../configure --disable-shared "LDFLAGS=-L$(GFLAGS_PATH)/build/lib -L$(GTEST_PATH)/build/googlemock/gtest" "CPPFLAGS=-I$(GFLAGS_PATH)/build/include -I$(GTEST_PATH)/googletest/include"; fi; make -j $(PROCESSOR_NUMS); echo 'build' > $(GLOG_PATH)/.gitignore;

clean:
	rm -rf $(OUTPUT)
	rm -rf $(CLEAN_FILES)
	find $(SRC_PATH) -name "*.[oda]*" -exec rm -f {} \;
	find $(SRC_PATH) -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

distclean: clean
	make -C $(PINK_PATH)/pink/ SLASH_PATH=$(SLASH_PATH) clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(NEMO_PATH) NEMODB_PATH=$(NEMODB_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) clean
	make -C $(NEMODB_PATH)/ ROCKSDB_PATH=$(ROCKSDB_PATH) clean
	make -C $(ROCKSDB_PATH)/ clean
#	make -C $(GFLAGS_PATH)/build/ clean
#	make -C $(GTEST_PATH)/build/ clean
#	make -C $(GLOG_PATH)/build/ clean
