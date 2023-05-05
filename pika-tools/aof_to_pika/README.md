## README

#### Introduction
A tool to transfer data for redis from one to another, it also support any redis-like nosql like [pika](https://github.com/baotiao/pika). The transfor progress is based on redis aof file, which simply read the aof and batch send to the destination peer.

#### Feature

- Continuously read new content of aof as 'tail -f'
- Read response and keep statistics
- More efficiency

#### Usage

``` shell
make
cd output/tools && ./aof_to_pika -h # for more information
```
