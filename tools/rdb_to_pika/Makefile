CXX=g++

CXXFLAGS = -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX -Wall -W -DDEBUG -g -O2 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11


SRC_DIR=./

OUT_PUT=./

TARGET=redisConn

THIRD=./../third

HIREDIS_PATH=$(THIRD)/hiredis-vip
LIB_HIREDIS=$(HIREDIS_PATH)/libhiredis_vip.a 

INCLUDE_PATH=-I./    \
                             -I$(HIREDIS_PATH)       

BASE_OBJ := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJ += $(wildcard $(SRC_DIR)/*.cpp)
BASE_OBJ += $(wildcard $(SRC_DIR)/*.c)
OBJ=$(patsubst %.cc, %.o, $(BASE_OBJ))

all:$(TARGET)
	echo "successful"

$(TARGET) : $(OBJ) $(LIB_HIREDIS) 
	$(CXX) $(OBJ) $(LIB_HIREDIS) -o $(TARGET)
 
$(LIB_HIREDIS):
	make -C $(HIREDIS_PATH)

$(OBJ): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
.PHONEY:clean
clean: 
	rm -rf $(TARGET) *.o 
