CXX=g++
LDFLAGS= -lpthread -lrt -lsnappy -lz -lbz2
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

all: blackwidow_bench

ifndef BLACKWIDOW_PATH
  $(warning Warning: missing blackwidow path, using default)
  BLACKWIDOW_PATH=..
endif
BLACKWIDOW_INCLUDE_DIR=$(BLACKWIDOW_PATH)/include
BLACKWIDOW_LIBRARY=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

ifndef ROCKSDB_PATH
  $(warning Warning: missing rocksdb path, using default)
	ROCKSDB_PATH=../deps/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

CXXFLAGS+= -I$(BLACKWIDOW_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR)

DEP_LIBS = $(BLACKWIDOW_LIBRARY) $(ROCKSDB_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

blackwidow_bench: blackwidow_bench.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./blackwidow_bench
