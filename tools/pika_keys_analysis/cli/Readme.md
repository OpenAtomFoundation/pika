# What is this?
This is a tool to analyze the keys of a pika cluster.
# How to use?
## 1. Install
```shell
go build -o pika_keys_analysis main.go
```
## 2. Start
```shell
./pika_keys_analysis config.yaml
```
## 3. List big keys
```shell
bigKey
```
## 4. Apply Config
```shell
apply config.yaml
```
## 5. Compress Key
```shell
compress <key>
```
## 6. Decompress Key
- not save to pika
```shell
decompress <key>
```
- save to pika
```shell
decompress -s <key> 
```
## 7. Recover Key
```shell
recover <from> <to>
```
# Notice

When using compression and decompression functions, errors in operation may cause duplicate compression or decompression, and the files used for recovery may be overwritten. If they are overwritten, the decompress command can be used to reach a state where decompression cannot continue, and then continue to compress to use the recover command normally