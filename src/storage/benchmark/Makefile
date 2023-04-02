CXX=g++
LDFLAGS= -lpthread -lrt -lsnappy -lz -lbz2
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

all: storage_bench

ifndef STORAGE_PATH
  $(warning Warning: missing storage path, using default)
  STORAGE_PATH=..
endif
STORAGE_INCLUDE_DIR=$(STORAGE_PATH)/include
STORAGE_LIBRARY=$(STORAGE_PATH)/lib/libstorage.a

ifndef ROCKSDB_PATH
  $(warning Warning: missing rocksdb path, using default)
	ROCKSDB_PATH=../deps/rocksdb
endif
ROCKSDB_INCLUDE_DIR=$(ROCKSDB_PATH)/include
ROCKSDB_LIBRARY=$(ROCKSDB_PATH)/librocksdb.a

CXXFLAGS+= -I$(STORAGE_INCLUDE_DIR) -I$(ROCKSDB_INCLUDE_DIR)

DEP_LIBS = $(STORAGE_LIBRARY) $(ROCKSDB_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

storage_bench: storage_bench.cc
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./storage_bench
