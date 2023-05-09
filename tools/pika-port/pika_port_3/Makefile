CXX = g++

CXXFLAGS = -O2 -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith \
	  -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter \
	  -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls

# CXXFLAGS = -O0 -g -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch \
# 	   -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE \
# 	   -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls

OBJECT = pika_port
SRC_DIR = .
THIRD_PATH = ../../third

ROCKSDB_PATH=$(THIRD_PATH)/rocksdb
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

SLASH_PATH=$(THIRD_PATH)/slash
SLASH=$(SLASH_PATH)/slash/lib/libslash.a

PINK_PATH=$(THIRD_PATH)/net
PINK=$(PINK_PATH)/net/lib/libnet.a

BLACKWIDOW_PATH=$(THIRD_PATH)/blackwidow
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

INCLUDE_PATH = -I./ \
			   -I$(THIRD_PATH)/slash/ \
			   -I$(THIRD_PATH)/net \
			   -I$(THIRD_PATH)/blackwidow/include/ \
			   -I$(THIRD_PATH)/blackwidow/ \
			   -I$(THIRD_PATH)/rocksdb/include/

LIB_PATH = -L./ \
  -L$(THIRD_PATH)/rocksdb/ \
  -L$(THIRD_PATH)/slash/slash/lib \
  -L$(THIRD_PATH)/net/net/lib \
  -L$(THIRD_PATH)/blackwidow/lib

LIBS = -Wl,-Bstatic -lrocksdb -Wl,-Bdynamic -lpthread -lrt -lnet -lslash -lblackwidow -lrocksdb -lglog -lgflags -lsnappy -lz -lbz2 -llz4 -lzstd

LIBS += $(ROCKSDB_LDFLAGS)

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(ROCKSDB) $(SLASH) $(PINK) $(BLACKWIDOW) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) -j 24 static_lib

$(SLASH) :
	make -C $(SLASH_PATH)/slash

$(PINK) :
	make -C $(PINK_PATH)/net NO_PB=1 SLASH_PATH=../../slash

$(BLACKWIDOW) :
	make -C $(BLACKWIDOW_PATH)  ROCKSDB_PATH=../rocksdb SLASH_PATH=../slash

clean:
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)
