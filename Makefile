CXX = g++
CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function
OBJECT = pika
SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/leveldb/include/ 

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/leveldb/


LIBS = -lpthread \
	   -lglog \
	   -lleveldb

GLOG = /usr/local/lib/libglog.a

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
	cp -r ./third/ $(OUTPUT)/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(GLOG) $(OBJS)
	make -C $(THIRD_PATH)/leveldb/
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(GLOG):
	cd $(THIRD_PATH)/glog; ./configure; make; echo '*' > $(CURDIR)/third/glog/.gitignore; sudo make install;

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
