
RPATH = /usr/local/pika11/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

OSVERSION := $(shell cat /etc/redhat-release | cut -d "." -f 1 | awk '{print $$NF}')
ifeq ($(OSVERSION), 5)
	SO_DIR = ./lib/5.4
	TOOLS_DIR = ./tools/5.4
else
	SO_DIR = ./lib/6.2
	TOOLS_DIR = ./tools/6.2
endif

CXX = g++
CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
OBJECT = pika
SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/nemo/output/include/ \
			   -I$(THIRD_PATH)/mario/output/include/\
			   -I$(THIRD_PATH)/libssh2/include/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/nemo/output/lib/ \
		   -L$(THIRD_PATH)/mario/output/lib/\
		   -L$(THIRD_PATH)/libssh2/src/


LIBS = -lpthread \
	   -lglog \
	   -lnemo \
	   -lmario \
	   -lrocksdb \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt \
	   -lssh2 \
	   -lssl \
	   -lcrypto

ROCKSDB = $(THIRD_PATH)/nemo/output/lib/librocksdb.a
GLOG = /usr/local/lib/libglog.a
MARIO = $(THIRD_PATH)/mario/output/lib/libmario.a
LIBSSH2 = $(THIRD_PATH)/libssh2/src/libssh2.a
NEMO_TOOLS = $(THIRD_PATH)/nemo/output/tools

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
	mkdir $(OUTPUT)/lib
	cp -r $(SO_DIR)/*  $(OUTPUT)/lib
	cp $(OBJECT) $(OUTPUT)/bin/
	mkdir $(OUTPUT)/tools
	cp -r $(TOOLS_DIR)/* $(OUTPUT)/tools/
	cp $(NEMO_TOOLS)/* $(OUTPUT)/tools/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(ROCKSDB) $(GLOG) $(MARIO) $(LIBSSH2) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH)  $(LFLAGS) $(LIBS) 

$(ROCKSDB):
	make -C $(THIRD_PATH)/nemo/

$(GLOG):
	cd $(THIRD_PATH)/glog; ./configure; make; echo '*' > $(CURDIR)/third/glog/.gitignore; sudo make install;

$(MARIO):
	make -C $(THIRD_PATH)/mario/ 

$(LIBSSH2):
	cd $(THIRD_PATH)/libssh2/; cmake .; make;

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
