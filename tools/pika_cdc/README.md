# Pika cdc
**A tool for incremental synchronization of pika command**

By imitating a pika slave

# Build
**Make sure the system has protoc installed** 
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Build pika cdc
```bash
make
```

## Todo:

Consumer side:
- [x] redis 
- [ ] kafka