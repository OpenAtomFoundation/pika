GCC = g++
CPPFLAGS = -Wall -W -Wno-unused-parameter -DDEBUG -D__XDEBUG__ -g -O3 -std=c++11
OBJECT = txt_to_pika

include ../../make_config.mk
LIB_PATH = -L ../../third/nemo/lib/ \
								-L ../../third/pink/pink/lib/ \
								-L ../../third/slash/slash/lib/ \
								-L ../../third/nemo-rocksdb/lib \
																																																																																				 -L ../../third/rocksdb/

LIBS = -Wl,-Bstatic -lnemo -lrocksdb\
								-Wl,-Bdynamic -lpthread\
								-lrt \
								-lpink \
								-lslash \
								-lnemodb \
								-lrocksdb

LIBS += $(ROCKSDB_LDFLAGS)


INCLUDE_PATH = -I../../third/pink/ \
												-I../../third/slash/ \
												-I../../third/nemo/include/ \
												-I../../third/nemo-rocksdb/include/ \
												-I../../third/rocksdb/include/


.PHONY: all clean

all: $(OBJECT)
				rm *.o

txt_to_pika : txt_to_pika.o scan.o sender.o
				$(GCC) $(CPPFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

%.o : %.cc
				$(GCC) $(CPPFLAGS) -c $< -o $@ $(INCLUDE_PATH)

clean:
				rm -rf $(OBJECT) $(OBJECT).o
