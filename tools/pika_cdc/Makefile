PROTO_OUT = ./pika
PROTO_DIR = ../../src/

GO_FILES = $(shell find . -name '*.go')

PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

OUTPUT_BIN = pika_cdc

PROTOC = protoc
GO_BUILD = go build
GO_CLEAN = go clean

.PHONY: all proto build clean

all: proto build

proto: $(PROTO_FILES)
	$(PROTOC) --proto_path=$(PROTO_DIR) --go_out=$(PROTO_OUT) $^

build: $(GO_FILES)
	$(GO_BUILD) -o $(OUTPUT_BIN)

clean:
	$(GO_CLEAN)
	rm -f $(OUTPUT_BIN)