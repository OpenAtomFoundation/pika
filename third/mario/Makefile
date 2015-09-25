CXX = g++

ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -D_GNU_SOURCE
else
	CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wconversion -D_GNU_SOURCE
endif

#ifeq ($(ENGINE), memory)
#	CXXFLAGS += -DMARIO_MEMORY
#else
#	CXXFLAGS += -DMARIO_MMAP
#endif

OBJECT = mario
SRC_DIR = ./src
TEST_DIR = ./test
OUTPUT = ./output
GTEST_DIR = ./gtest-1.7.0/

LIB_PATH = -L./
LIBS = $(GTEST_DIR)/libgtest.a \
	   -lpthread


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(GTEST_DIR)/include/ \
			   -I$(GTEST_DIR)/include/gtest

LIBRARY = libmario.a

.PHONY: all test clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

TEST_OBJS := $(wildcard $(TEST_DIR)/*.cc)
TOBJS = $(patsubst %.cc,%.o,$(TEST_OBJS))

all: $(LIBRARY)
	@echo "Success, go, go, go..."

test: all $(TESTS)
	for t in $(TESTS); do echo "***** Running $$t"; ./$$t || exit 1; done


$(TESTS): $(TOBJS)
	$(CXX) $(CXXFLAGS) $(GTEST_DIR)/src/gtest_main.cc $(INCLUDE_PATH) $(OBJS) $(TOBJS) -isystem  $(LIB_PATH) $(LIBS) -o $@ 


$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/include
	mkdir $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp -r ./include $(OUTPUT)/
	mv ./libmario.a $(OUTPUT)/lib/
	make -C example

$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

$(TOBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	make -C example clean
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(TEST_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
	rm -rf $(LIBRARY)
	rm -rf $(OBJECT)
	rm -rf $(TESTS)
