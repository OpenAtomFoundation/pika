### 概述

Pika Raft集群模式基于nuraft实现，具体配置方法可以看pika.conf中相关部分。

作为可开启功能，当关闭时pika依旧可以正常运行单机/主从模式。

### Raft Instance

目前整个Pika实例使用一个Raft实例管理，即所有的DB中的所有slot上的写请求都会通过同一个Raft实例管理。

原则上只有Leader可以处理写请求，Follower收到写请求会返回错误。在配置中提供了写请求重定向开关，开启后Follower端收到的写请求会重定向给Leader让Leader处理。

### Log

使用levelDB存储RaftLog。只有写请求会通过Raft维护，读请求从本地读取。

### Snapshot

参考主从同步的Rsync, 创建Snapshot时会调用bgsave获取所有数据库的dump文件，然后通过Raft将这些文件传递给落后的Follower。新的dump文件会覆盖旧的，因此这里只保留上一次的Snapshot元数据，也暂不支持异步的Snapshot创建。

### 暂不支持

1. Multi-Raft
2. 异步Snapshot
3. 动态配置变更