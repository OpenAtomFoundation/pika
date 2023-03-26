CXX=g++
LDFLAGS= -lpthread -lrt -lprotobuf
CXXFLAGS=-O2 -std=c++11 -fno-builtin-memcmp -msse -msse4.2

.PHONY: clean all

all: bg_thread http_server mydispatch_srv myholy_srv myholy_srv_chandle myproto_cli \
	redis_cli_test simple_http_server myredis_srv myredis_cli redis_parser_test binlog_parser_test \
	thread_pool_test

ifndef PINK_PATH
  $(warning Warning: missing pink path, using default)
  PINK_PATH=$(CURDIR)/../..
endif
PINK_INCLUDE_DIR=$(PINK_PATH)
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a

ifndef PSTD_PATH
  $(warning Warning: missing pstd path, using default)
	PSTD_PATH=$(CURDIR)/../third/pstd
endif
PSTD_INCLUDE_DIR=$(PSTD_PATH)
PSTD_LIBRARY=$(PSTD_PATH)/pstd/lib/libpstd.a

CXXFLAGS+= -I$(PINK_INCLUDE_DIR) -I$(PSTD_INCLUDE_DIR)

DEP_LIBS = $(PINK_LIBRARY) $(PSTD_LIBRARY)
LDFLAGS := $(DEP_LIBS) $(LDFLAGS)

bg_thread: bg_thread.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

thread_pool_test: thread_pool_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

http_server: http_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

https_server: https_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

mydispatch_srv: mydispatch_srv.cc myproto.pb.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myholy_srv: myholy_srv.cc myproto.pb.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myholy_srv_chandle: myholy_srv_chandle.cc myproto.pb.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myproto_cli: myproto_cli.cc myproto.pb.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

redis_cli_test: redis_cli_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

redis_parser_test: redis_parser_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

binlog_parser_test: binlog_parser_test.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

simple_http_server: simple_http_server.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myredis_srv: myredis_srv.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myredis_cli: myredis_cli.cc
	$(CXX) $(CXXFLAGS) $^ -o$@ $(LDFLAGS)

myproto.pb.cc: myproto.proto
	protoc -I=. --cpp_out=. ./$^

clean:
	find . -name "*.[oda]" -exec rm -f {} \;
	rm -rf ./bg_thread ./http_server ./https_server ./mydispatch_srv ./myholy_srv \
					./myholy_srv_chandle ./myproto_cli ./redis_cli_test ./simple_http_server \
					./redis_parser_test ./myredis_srv ./myredis_cli ./binlog_parser_test ./thread_pool_test
