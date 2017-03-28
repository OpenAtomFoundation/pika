CXX = g++
CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
OBJECT = ssdb_to_pika
SRC_DIR = .
OUTPUT = .

INCLUDE_PATH = -I./ \
							 -I../../third/nemo/output/include \
							 -I../../third/nemo/3rdparty/nemo-rocksdb/output/include \
							 -I../../third/nemo/3rdparty/nemo-rocksdb/rocksdb \
							 -I../../third/nemo/3rdparty/nemo-rocksdb/rocksdb/include

STATIC_LIBS = ./libssdb-client.a \
					    ../../third/nemo/output/lib/libnemo.a \
					    ../../third/nemo/output/lib/libnemodb.a \
					    ../../third/nemo/output/lib/librocksdb.a

LIBS = -lsnappy \
			 -lrt \
			 -lz \
			 -lbz2 \
			 -lpthread

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(STATIC_LIBS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)
