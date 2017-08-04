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

GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0.0.0
PINK = $(THIRD_PATH)/pink/pink/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/slash/lib/libslash.a

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


$(FUNC_OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
$(MAIN_OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 
	
clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OBJECT)

