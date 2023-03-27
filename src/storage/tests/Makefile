CXX=g++
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2
OPT=

.PHONY: clean all

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
OPT += -O2 -DNDEBUG
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += -O0
endif

#-----------------------------------------------

# Get processor numbers
dummy := $(shell ("$(CURDIR)/../detect_environment" "$(CURDIR)/make_config.mk"))
include make_config.mk
LDFLAGS = -lpthread -lrt $(ROCKSDB_LDFLAGS)

ifndef BLACKWIDOW_PATH
  $(warning Warning: missing storage path, using default)
  BLACKWIDOW_PATH=..
endif
BLACKWIDOW_INCLUDE_DIR=$(BLACKWIDOW_PATH)/include
BLACKWIDOW_LIBRARY=$(BLACKWIDOW_PATH)/lib/libstorage.a

ifndef ROCKSDB_PATH
  $(warning Warning: missing rocksdb path, using default)
	ROCKSDB_PATH=../deps/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

ifndef SLASH_PATH
  $(warning Warning: missing pstd path, using default)
	SLASH_PATH=../deps/pstd
endif
SLASH_INCLUDE_DIR=$(SLASH_PATH)
SLASH_LIBRARY=$(SLASH_PATH)/pstd/lib/libpstd.a

ifndef GOOGLETEST_PATH
  $(warning Warning: missing googletest path, using default)
	GOOGLETEST_PATH=../deps/googletest/googletest
endif
GOOGLETEST_INCLUDE_DIR=$(GOOGLETEST_PATH)/include
GOOGLETEST_LIBRARY=$(GOOGLETEST_PATH)/libgtest.a

CXXFLAGS+= -I$(BLACKWIDOW_PATH) -I$(BLACKWIDOW_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR) -I$(SLASH_INCLUDE_DIR) -I$(GOOGLETEST_INCLUDE_DIR) $(OPT)

DEP_LIBS = $(BLACKWIDOW_LIBRARY) $(ROCKSDB_LIBRARY) $(SLASH_LIBRARY) $(GOOGLETEST_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

OBJECTS= GOOGLETEST ROCKSDB SLASH main lock_mgr gtest_keys gtest_strings gtest_hashes gtest_lists gtest_sets gtest_zsets gtest_strings_filter gtest_hashes_filter gtest_hyperloglog gtest_lists_filter gtest_custom_comparator gtest_lru_cache gtest_options

all: $(OBJECTS)

dbg: $(OBJECTS)

test: $(OBJECTS)
	@rm -rf db
	@mkdir -p db/keys db/strings db/hashes db/hash_meta db/sets db/hyperloglog db/list_meta db/lists db/zsets
	@./gtest_keys
	@./gtest_strings
	@./gtest_hashes
	@./gtest_lists
	@./gtest_sets
	@./gtest_zsets
	@./gtest_strings_filter
	@./gtest_hashes_filter
	@./gtest_lists_filter
	@./gtest_hyperloglog
	@./gtest_custom_comparator
	@./gtest_lru_cache
	@./gtest_options
	@rm -rf db

GOOGLETEST:
	@cd $(GOOGLETEST_PATH); if [ ! -f ./Makefile ]; then  cmake .; fi; make;

ROCKSDB:
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH)/ static_lib

SLASH:
	$(AM_V_at)make -C $(SLASH_PATH)/pstd

main: main.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

lock_mgr: lock_mgr.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_keys: gtest_keys.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_strings: gtest_strings.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_hashes: gtest_hashes.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_lists: gtest_lists.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_sets: gtest_sets.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_zsets: gtest_zsets.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_strings_filter: gtest_strings_filter.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_hashes_filter: gtest_hashes_filter.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_custom_comparator: gtest_custom_comparator.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_lists_filter: gtest_lists_filter.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_hyperloglog: gtest_hyperloglog.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_lru_cache: gtest_lru_cache.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

gtest_options: gtest_options.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)


clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -f ./make_config.mk
	rm -rf db
	rm -rf ./main ./lock_mgr ./gtest_keys ./gtest_strings ./gtest_hashes ./gtest_lists ./gtest_sets ./gtest_zsets ./gtest_strings_filter ./gtest_hashes_filter ./gtest_hyperloglog ./gtest_lists_filter ./gtest_custom_comparator ./gtest_lru_cache ./gtest_options
