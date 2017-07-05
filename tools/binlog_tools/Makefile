
include ../../Makefile.global

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
	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
endif

OBJECT = binlog_parser binlog_sender
SRC_DIR = .
THIRD_PATH = ../../third

INCLUDE_PATH = -I../../include/ \
			   -I../../src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/slash \
			   -I$(THIRD_PATH)/pink

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/slash/slash/lib/ \
		   -L$(THIRD_PATH)/pink/pink/lib/ \
		   -L$(THIRD_PATH)/glog/.libs/


LIBS = -lpthread \
	   -lglog \
	   -lslash \
		 -lpink

GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0
PINK = $(THIRD_PATH)/pink/pink/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/pink/lib/libslash.a

.PHONY: all clean


BASE_SRCS := $(wildcard $(SRC_DIR)/*.cc)
BASE_SRCS += $(wildcard $(SRC_DIR)/*.c)
BASE_SRCS += $(wildcard $(SRC_DIR)/*.cpp)

MAIN_SRCS = $(patsubst %,./%.cc,$(OBJECT))
FUNC_SRCS = $(filter-out $(MAIN_SRCS),$(BASE_SRCS)) 
FUNC_OBJS = $(patsubst %.cc,%.o,$(FUNC_SRCS))
MAIN_OBJS = $(patsubst %.cc,%.o,$(MAIN_SRCS))


all: $(OBJECT)
	@echo "UNAME    : $(UNAME)"
	@echo "SO_DIR   : $(SO_DIR)"
	@echo "TOOLS_DIR: $(TOOLS_DIR)"
	@echo "Success, go, go, go..."


$(OBJECT): $(GLOG) $(PINK) $(SLASH) $(FUNC_OBJS) $(MAIN_OBJS) 
	$(CXX) $(CXXFLAGS) -o $@ $@.o $(FUNC_OBJS) $(INCLUDE_PATH) $(LIB_PATH)  $(LFLAGS) $(LIBS) 


$(SLASH):
	make -C $(THIRD_PATH)/slash/slash

$(PINK):
	make -C $(THIRD_PATH)/pink/pink SLASH_PATH=$(THIRD_PATH)/slash

$(FUNC_OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
$(MAIN_OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(GLOG):
	cd $(THIRD_PATH)/glog; if [ ! -f ./Makefile ]; then ./configure; fi; make; echo '*' > $(CURDIR)/third/glog/.gitignore; cp $(CURDIR)/third/glog/.libs/libglog.so.0 $(SO_DIR);
	
clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)

