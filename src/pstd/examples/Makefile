CXX=g++
LDFLAGS= -lpthread -lrt
CXXFLAGS= -O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean libpstd

all: conf_example cond_lock_example binlog_example mutex_example hash_example

CXXFLAGS+= -I../..

conf_example: libpstd conf_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libpstd.a $(LDFLAGS)

cond_lock_example: libpstd cond_lock_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libpstd.a $(LDFLAGS)

binlog_example: libpstd binlog_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libpstd.a $(LDFLAGS)

mutex_example: libpstd mutex_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libpstd.a $(LDFLAGS)

hash_example: libpstd hash_example.cc
	$(CXX) $(CXXFLAGS) $@.cc -o$@ ../lib/libpstd.a $(LDFLAGS)

clean:
	find . -name "*.[oda]*" -exec rm -f {} \;
	rm -rf ./conf_example ./cond_lock_example ./binlog_example ./mutex_example ./hash_example

libpstd:
	cd .. && $(MAKE) static_lib
