CXX = g++

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
	-Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

CXXFLAGS = $(WARNING_FLAGS) -std=c++11 -g

TARGET = txt_to_pika 

THIRD_PATH=../third
SRC_PATH=.

ROCKSDB_PATH=$(THIRD_PATH)/rocksdb
ROCKSDB=$(ROCKSDB_PATH)/librocksdb.a

SLASH_PATH=$(THIRD_PATH)/slash
SLASH=$(SLASH_PATH)/slash/lib/libslash.a

PINK_PATH=$(THIRD_PATH)/pink
PINK=$(PINK_PATH)/pink/lib/libpink.a

BLACKWIDOW_PATH=$(THIRD_PATH)/blackwidow
BLACKWIDOW=$(BLACKWIDOW_PATH)/lib/libblackwidow.a

INCLUDE_PATH = -I./                             \
               -I$(BLACKWIDOW_PATH)/include     \
               -I$(SLASH_PATH)                  \
               -I$(PINK_PATH)                   \
               -I$(ROCKSDB_PATH)/include        \

LIB_PATH     = -L$(BLACKWIDOW_PATH)/lib         \
               -L$(PINK_PATH)/pink/lib          \
               -L$(SLASH_PATH)/slash/lib        \
               -L$(ROCKSDB_PATH)/               \

LIBS         = -lblackwidow                     \
               -lpink                           \
               -lslash                          \
               -lrocksdb                        \
               -lpthread                        \
               -lsnappy                         \
               -lz                              \
               -lbz2                            \
               -lglog                           \
               -lgflags                         \
               -lrt                             \

SOURCE := $(wildcard $(SRC_PATH)/*.cc)
OBJS := $(patsubst %.cc, %.o, $(SOURCE))

$(TARGET): $(OBJS) $(BLACKWIDOW) $(PINK) $(SLASH) $(ROCKSDB)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)


$(BLACKWIDOW) :
	make -C $(BLACKWIDOW_PATH) ROCKSDB_PATH=../rocksdb SLASH_PATH=../slash

$(PINK) :
	make -C $(PINK_PATH)/pink SLASH_PATH=../../slash

$(SLASH) :
	make -C $(SLASH_PATH)/slash

$(ROCKSDB):
	make -C $(ROCKSDB_PATH) -j 24 static_lib


.PHONY: clean distclean

clean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)

distclean:
	rm -rf $(OBJS)
	rm -rf $(TARGET)
	make -C $(BLACKWIDOW_PATH) clean
	make -C $(PINK_PATH)/pink clean
	make -C $(SLASH_PATH)/slash clean
	make -C $(ROCKSDB_PATH) clean
