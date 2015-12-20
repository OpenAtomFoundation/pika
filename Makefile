CXX = g++
CXXFLAGS = -Wall -W -DDEBUG -g -gstabs+ -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
OBJECT = pika
SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/nemo/output/include/ \
			   -I$(THIRD_PATH)/mario/output/include/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/nemo/output/lib/ \
		   -L$(THIRD_PATH)/nemo/3rdparty/rocksdb/ \
		   -L$(THIRD_PATH)/mario/output/lib/


LIBS = -lpthread \
	   -lglog \
	   -lnemo \
	   -lmario \
	   -lrocksdb \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

ROCKSDB = $(THIRD_PATH)/nemo/output/lib/librocksdb.a
GLOG = /usr/local/lib/libglog.a
MARIO = $(THIRD_PATH)/mario/output/lib/libmario.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	cp -r ./conf $(OUTPUT)/
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(ROCKSDB) $(GLOG) $(MARIO) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(ROCKSDB):
	make -C $(THIRD_PATH)/nemo/

$(GLOG):
	cd $(THIRD_PATH)/glog; ./configure; make; echo '*' > $(CURDIR)/third/glog/.gitignore; sudo make install;

$(MARIO):
	make -C $(THIRD_PATH)/mario/ 

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
