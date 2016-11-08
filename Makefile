
include Makefile.global
#RPATH = /usr/local/pika21/lib/
#LFLAGS = -Wl,-rpath=$(RPATH)


UNAME := $(shell if [ -f "/etc/redhat-release" ]; then echo "CentOS"; else echo "Ubuntu"; fi)

OSVERSION := $(shell cat /etc/redhat-release | cut -d "." -f 1 | awk '{print $$NF}')

ifeq ($(UNAME), Ubuntu)
  SO_DIR = $(CURDIR)/lib/ubuntu
  TOOLS_DIR = $(CURDIR)/tools/ubuntu
else ifeq ($(OSVERSION), 5)
  SO_DIR = $(CURDIR)/lib/5.4
  TOOLS_DIR = $(CURDIR)/tools/5.4
else
  SO_DIR = $(CURDIR)/lib/6.2
  TOOLS_DIR = $(CURDIR)/tools/6.2
endif

CXX = g++

ifeq ($(__REL), 1)
#CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
	CXXFLAGS = -O2 -g -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
endif

OBJECT = pika
SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/nemo/output/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/pink/output/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/nemo/output/lib/ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/ \
		   -L$(THIRD_PATH)/glog/.libs/


LIBS = -lpthread \
	   -lglog \
	   -lnemo \
	   -lslash \
	   -lrocksdb \
		 -lpink \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

NEMO = $(THIRD_PATH)/nemo/output/lib/libnemo.a
#GLOG = $(SO_DIR)/libglog.so.0
GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0
PINK = $(THIRD_PATH)/pink/output/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/output/lib/libslash.a

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	@echo "UNAME    : $(UNAME)"
	@echo "SO_DIR   : $(SO_DIR)"
	@echo "TOOLS_DIR: $(TOOLS_DIR)"
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	cp -r ./conf $(OUTPUT)/
	mkdir $(OUTPUT)/lib
	cp -r $(SO_DIR)/*  $(OUTPUT)/lib
	cp $(OBJECT) $(OUTPUT)/bin/
	mkdir $(OUTPUT)/tools
	if [ -d $(TOOLS_DIR) ]; then \
		cp -r $(TOOLS_DIR)/* $(OUTPUT)/tools/; \
	fi
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(NEMO) $(GLOG) $(PINK) $(SLASH) $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIB_PATH)  $(LFLAGS) $(LIBS) 

$(NEMO):
	make -C $(THIRD_PATH)/nemo/

$(SLASH):
	make -C $(THIRD_PATH)/slash/

$(PINK):
	make -C $(THIRD_PATH)/pink/

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(GLOG):
	#cd $(THIRD_PATH)/glog; ./configure; make; echo '*' > $(CURDIR)/third/glog/.gitignore; cp $(CURDIR)/third/glog/.libs/libglog.so.0 $(SO_DIR);
	cd $(THIRD_PATH)/glog; if [ ! -f ./Makefile ]; then ./configure; fi; make; echo '*' > $(CURDIR)/third/glog/.gitignore; cp $(CURDIR)/third/glog/.libs/libglog.so.0 $(SO_DIR);
	
clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)

