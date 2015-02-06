CXX = g++
CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG1__ -fPIC -Wno-unused-function
OBJECT = tick
SRC_DIR = ./src
THIRD_PATH = ./third/
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog-0.3.3/src/ \
			   -I$(THIRD_PATH)/leveldb/include/ 

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/glog-0.3.3/ \
		   -L/usr/local/lib/ \
		   -L$(THIRD_PATH)/leveldb/


LIBS = -lpthread \
	   -lprotobuf \
	   -lleveldb

DYNAMIC_LIBS = -lglog

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	mkdir $(OUTPUT)/log
	mkdir $(OUTPUT)/third
	cp -r ./conf $(OUTPUT)/
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS) $(DYNAMIC_LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
