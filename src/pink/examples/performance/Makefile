ifndef SLASH_PATH
SLASH_PATH=../../third/slash
endif

ifndef PINK_PATH
PINK_PATH=../../../
endif

CXXFLAGS= -O2 -I$(PINK_PATH) -I$(SLASH_PATH) -std=c++11
LDFLAGS= -L$(SLASH_PATH)/slash/lib -L$(PINK_PATH)/pink/lib -lprotobuf -lpink -lslash -lpthread

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
