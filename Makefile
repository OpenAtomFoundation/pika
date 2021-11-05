CLEAN_FILES = # deliberately empty, so we can append below.
CXX=g++
PLATFORM_LDFLAGS= -lpthread -lrt
PLATFORM_CXXFLAGS= -std=c++11 -fno-builtin-memcmp -msse -msse4.2 
ROCKSDB_CXXFLAGS= -Wno-error=deprecated-copy -Wno-error=pessimizing-move -Wno-error=class-memaccess -Wno-error=array-bounds
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
ROCKSDB = $(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a

ifndef GLOG_PATH
GLOG_PATH = $(THIRD_PATH)/glog
endif

ifndef BLACKWIDOW_PATH
BLACKWIDOW_PATH = $(THIRD_PATH)/blackwidow
endif
BLACKWIDOW = $(BLACKWIDOW_PATH)/lib/libblackwidow$(DEBUG_SUFFIX).a


ifeq ($(360), 1)
GLOG := $(GLOG_PATH)/.libs/libglog.a
endif

INCLUDE_PATH = -I. \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH) \
							 -I$(BLACKWIDOW_PATH)/include \
							 -I$(ROCKSDB_PATH) \
							 -I$(ROCKSDB_PATH)/include \

ifeq ($(360),1)
INCLUDE_PATH += -I$(GLOG_PATH)/src
endif

LIB_PATH = -L./ \
					 -L$(SLASH_PATH)/slash/lib \
					 -L$(PINK_PATH)/pink/lib \
					 -L$(BLACKWIDOW_PATH)/lib \
					 -L$(ROCKSDB_PATH)        \

ifeq ($(360),1)
LIB_PATH += -L$(GLOG_PATH)/.libs
endif

LDFLAGS += $(LIB_PATH) \
			 		 -lpink$(DEBUG_SUFFIX) \
			 		 -lslash$(DEBUG_SUFFIX) \
					 -lblackwidow$(DEBUG_SUFFIX) \
					 -lrocksdb$(DEBUG_SUFFIX) \
					 -lglog \
					 -lprotobuf \

# ---------------End Dependences----------------

VERSION_CC=$(SRC_PATH)/build_version.cc
LIB_SOURCES :=  $(VERSION_CC) \
				$(filter-out $(VERSION_CC), $(wildcard $(SRC_PATH)/*.cc))

PIKA_PROTO := $(wildcard $(SRC_PATH)/*.proto)
PIKA_PROTO_GENS:= $(PIKA_PROTO:%.proto=%.pb.h) $(PIKA_PROTO:%.proto=%.pb.cc)


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
PROTOOBJECTS = $(PIKA_PROTO:.proto=.pb.o)

# if user didn't config LIBNAME, set the default
ifeq ($(BINNAME),)
# we should only run pika in production with DEBUG_LEVEL 0
BINNAME=pika$(DEBUG_SUFFIX)
endif
BINARY = ${BINNAME}

.PHONY: distclean clean dbg all

%.pb.h %.pb.cc: %.proto
	$(AM_V_GEN)protoc --proto_path=$(SRC_PATH) --cpp_out=$(SRC_PATH) $<

%.o: %.cc
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

proto: $(PIKA_PROTO_GENS)

all: $(BINARY)

dbg: $(BINARY)

$(BINARY): $(SLASH) $(PINK) $(ROCKSDB) $(BLACKWIDOW) $(GLOG) $(PROTOOBJECTS) $(LIBOBJECTS)
	$(AM_V_at)rm -f $@
	$(AM_V_at)$(AM_LINK)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)mkdir -p $(OUTPUT)/bin
	$(AM_V_at)mv $@ $(OUTPUT)/bin
	$(AM_V_at)cp -r $(CURDIR)/conf $(OUTPUT)
	

$(SLASH):
	$(AM_V_at)make -C $(SLASH_PATH)/slash/ DEBUG_LEVEL=$(DEBUG_LEVEL)

$(PINK):
	$(AM_V_at)make -C $(PINK_PATH)/pink/ DEBUG_LEVEL=$(DEBUG_LEVEL) NO_PB=0 SLASH_PATH=$(SLASH_PATH)

$(ROCKSDB):
	$(AM_V_at) CXXFLAGS='$(ROCKSDB_CXXFLAGS)' make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ static_lib DISABLE_JEMALLOC=1 DEBUG_LEVEL=$(DEBUG_LEVEL) 

$(BLACKWIDOW):
	$(AM_V_at)make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) SLASH_PATH=$(SLASH_PATH) DEBUG_LEVEL=$(DEBUG_LEVEL)

$(GLOG):
	cd $(THIRD_PATH)/glog; if [ ! -f ./Makefile ]; then ./configure --disable-shared; fi; make; echo '*' > $(CURDIR)/third/glog/.gitignore;

clean:
	rm -rf $(OUTPUT)
	rm -rf $(CLEAN_FILES)
	rm -rf $(PIKA_PROTO_GENS)
	find $(SRC_PATH) -name "*.[oda]*" -exec rm -f {} \;
	find $(SRC_PATH) -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

distclean: clean
	make -C $(PINK_PATH)/pink/ SLASH_PATH=$(SLASH_PATH) clean
	make -C $(SLASH_PATH)/slash/ clean
	make -C $(BLACKWIDOW_PATH)/ clean
	make -C $(ROCKSDB_PATH)/ clean
#	make -C $(GLOG_PATH)/ clean
