GCC = g++
#CPPFLAGS = -Wall -W -Wno-unused-parameter -g -O2 -std=c++11
CPPFLAGS = -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O2 -std=c++11
OBJECT = pika_to_redis
#p2r_test_correct

LIB_PATH = -L ../../third/nemo/output/lib/ \
					 -L ../../third/pink/pink/lib/ \
					 -L ../../third/slash/slash/lib/ \
					 -L ../../third/nemo/3rdparty/nemo-rocksdb/output/lib \

LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
			 -Wl,-Bdynamic -lpthread\
			 -lsnappy \
			 -lrt \
			 -lz \
			 -lbz2 \
			 -lpink \
			 -lslash \
			 -lnemodb \
			 -lrocksdb \


INCLUDE_PATH = -I../../third/pink/ \
			   -I../../third/slash/ \
			   -I../../third/nemo/include/ \
			   -I../../third/nemo/3rdparty/nemo-rocksdb/include/ \
			   -I../../third/nemo/3rdparty/nemo-rocksdb/rocksdb/include/ \


.PHONY: all clean


# BASE_BOJS := $(wildcard *.cpp)
# BASE_BOJS += $(wildcard *.c)
# OBJS := $(patsubst %.cpp,%.o,$(BASE_BOJS))


all: $(OBJECT)
	rm *.o

#$(OBJECT): $(OBJECT).o
pika_to_redis : pika_to_redis.o migrator_thread.o sender.o
	$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
	$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
	rm -rf $(OBJECT) $(OBJECT).o
