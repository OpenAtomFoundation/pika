# PikiwiDB

[Click me switch to English](README.en.md)

C++11实现的增强版Redis服务器,使用Leveldb作为持久化存储引擎。(集群支持尚正在计划中)

## 环境需求
* C++11、CMake
* Linux 或 MAC OS

## 与Redis完全兼容
 你可以用redis的各种工具来测试PikiwiDB，比如官方的redis-cli, redis-benchmark。

 PikiwiDB可以和redis之间进行复制，可以读取redis的rdb文件或aof文件。当然，PikiwiDB生成的aof或rdb文件也可以被redis读取。

 你还可以用redis-sentinel来实现PikiwiDB的高可用！

 总之，PikiwiDB与Redis完全兼容。

## 高性能
- PikiwiDB性能大约比Redis3.2高出20%(使用redis-benchmark测试pipeline请求，比如设置-P=50或更高)
- PikiwiDB的高性能有一部分得益于独立的网络线程处理IO，因此和redis比占了便宜。但PikiwiDB逻辑仍然是单线程的。
- 另一部分得益于C++ STL的高效率（CLANG的表现比GCC更好）。
- 在测试前，你要确保std::list的size()是O(1)复杂度，这才遵循C++11的标准。否则list相关命令不可测。

运行下面这个命令，试试和redis比一比~
```bash
./redis-benchmark -q -n 1000000 -P 50 -c 50
```

## 编写扩展模块
 PikiwiDB支持动态库模块，可以在运行时添加新命令。
 我添加了三个命令(ldel, skeys, hgets)作为演示。

## 支持冷数据淘汰
 是的，在内存受限的情况下，你可以让PikiwiDB根据简单的LRU算法淘汰一些key以释放内存。

## 主从复制，事务，RDB/AOF持久化，慢日志，发布订阅
 这些特性PikiwiDB都有:-)

## 持久化：内存不再是上限
 Leveldb可以配置为PikiwiDB的持久化存储引擎，可以存储更多的数据。


## 命令列表
#### 展示PikiwiDB支持的所有命令
- cmdlist

#### key commands
- type exists del expire pexpire expireat pexpireat ttl pttl persist move keys randomkey rename renamenx scan sort

#### server commands
- select dbsize bgsave save lastsave flushdb flushall client debug shutdown bgrewriteaof ping echo info monitor auth

#### string commands
- set get getrange setrange getset append bitcount bitop getbit setbit incr incrby incrbyfloat decr decrby mget mset msetnx setnx setex psetex strlen

#### list commands
- lpush rpush lpushx rpushx lpop rpop lindex llen lset ltrim lrange linsert lrem rpoplpush blpop brpop brpoplpush

#### hash commands
- hget hmget hgetall hset hsetnx hmset hlen hexists hkeys hvals hdel hincrby hincrbyfloat hscan hstrlen

#### set commands
- sadd scard srem sismember smembers sdiff sdiffstore sinter sinterstore sunion sunionstore smove spop srandmember sscan

#### sorted set commands
- zadd zcard zrank zrevrank zrem zincrby zscore zrange zrevrange zrangebyscore zrevrangebyscore zremrangebyrank zremrangebyscore

#### pubsub commands
- subscribe unsubscribe publish psubscribe punsubscribe pubsub

#### multi commands
- watch unwatch multi exec discard

#### replication commands
- sync slaveof


