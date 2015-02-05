GCC = g++
CPPFLAGS = -Wall -W -DDEBUG -g -O2 
LDFLAGS = 
OBJECT = t

LIB_PATH = -L ../ \
			
LIBS = -lleveldb \
	   -lpthread

INCLUDE_PATH = -I../include/

.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS)) 

OBJS = thread.o

all: $(OBJECT)
	rm *.o

thread: $(OBJS)
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

snap: snap.o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

t: t.o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

recovery: recovery.o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LDFLAGS) $(LIBS)

%.o : %.cpp
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH) $(LIBS)

clean:
	rm -rf $(OBJECT) ./a.out
