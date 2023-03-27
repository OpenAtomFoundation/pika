ifndef SLASH_PATH
SLASH_PATH=../../third/pstd
endif

ifndef NET_PATH
NET_PATH=../../../
endif

CXXFLAGS= -O2 -I$(NET_PATH) -I$(SLASH_PATH) -std=c++11
LDFLAGS= -L$(SLASH_PATH)/pstd/lib -L$(NET_PATH)/net/lib -lprotobuf -lnet -lpstd -lpthread

.PHONY: all

all: server client

server: message.pb.o server.o
	$(CXX) -o $@ $^ $(LDFLAGS)

client: message.pb.o client.o
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.cc
	$(CXX) -c $< $(CXXFLAGS)

message.pb.cc:
	protoc --proto_path=./ --cpp_out=./ ./message.proto

clean:
	rm -f server client *.o message.pb.*
