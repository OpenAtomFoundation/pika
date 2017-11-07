CXX = g++
CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
OBJECT = aof_to_pika
SRC_DIR = ./src
OUTPUT = ./output

INCLUDE_PATH = -I./include -I../..

LIBS = -lpthread 

.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(INCLUDE_PATH) $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
