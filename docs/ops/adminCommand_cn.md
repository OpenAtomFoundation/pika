## client kill all
删除全部的客户端

```
xxx.qihoo.net:8221> client kill all
OK
xxx.qihoo.net:8221>
```

## bgsave
执行方式和redis一样。但是异步dump完后，数据库保存在dump_path目录下，dump出来的数据库包含dump_prefix和dump时间等信息；

```
xxx.qihoo.net:8221> BGSAVE
20160422134755 : 2213: 32582292
```
返回的信息包括dump的时间（20160422134755）和当前的binlog位置，即文件号：偏移量（2213: 32582292）
    
```
xxx.qihoo.net # ll /data3/pika_test/dump/
总用量 0
drwxr-xr-x 1 root root 42 4月  22 13:47 pika8221-20160422
```
"/data3/pika_test/dump/"是dump的路径，"pika9221-"为dump_prefix，20160422是dump的日期

## delbackup
删除dump目录下除正在使用（全同步中）的db快照外的其他快照

```
xxx.qihoo.net:8221> DELBACKUP
OK
```

## info keyspace
执行方式是“info keyspace 1”，“info keyspace 0”和”info keyspace“， “info keyspace”和“info keyspace 0”等价；  
info keyspace 1： 异步开始一次keyspace的扫描，并返回已完成的上一次keyspace扫描的结果  
info keyspace 0: 返回已完成的上一次keyspace扫描的结果  

```
xxx.qihoo.net:8221> info keyspace 1
# Keyspace
# Time:1970-01-01 08:00:00
kv keys:0
hash keys:0
list keys:0
zset keys:0
set keys:0
xxx.qihoo.net:8221> info keyspace
# Keyspace
# Time:2016-04-22 13:45:54
kv keys:13
hash keys:0
list keys:0
zset keys:0
set keys:0
```
 
## config get/set *
config get和config set的用法和redis是一样的，但是选项可能会有所不同，所以配了两个命令  
1) config get *  
2) config set *
用于分别列出config get和config set所能操作的选项

```
xxx.qihoo.net:8221> config get *
 1) "port"
 2) "thread_num"
 3) "log_path"
 4) "log_level"
 5) "db_path"
 6) "maxmemory"
 7) "write_buffer_size"
 8) "timeout"
 9) "requirepass"
10) "userpass"
11) "userblacklist"
12) "daemonize"
13) "dump_path"
14) "dump_prefix"
15) "pidfile"
16) "maxconnection"
17) "target_file_size_base"
18) "expire_logs_days"
19) "expire_logs_nums"
20) "root_connection_num"
21) "slowlog_log_slower_than"
22) "slave-read-only"
23) "binlog_file_size"
24) "compression"
25) "db_sync_path"
26) "db_sync_speed"
xxx.qihoo.net:8221> config set *
 1) "log_level"
 2) "timeout"
 3) "requirepass"
 4) "userpass"
 5) "userblacklist"
 6) "dump_prefix"
 7) "maxconnection"
 8) "expire_logs_days"
 9) "expire_logs_nums"
10) "root_connection_num"
11) "slowlog_log_slower_than"
12) "slave-read-only"
13) "db_sync_speed"
bada06.add.zwt.qihoo.net:8221>
```
## compact
因为pika底层存储引擎是基于rocksdb改造来的，会存在读写和空间放大的问题，除了rocksdb的自动compaction，pika也设置了一个手动compaction的命令，以强制compact整个kespace内的内容，支持对单个数据结构进行compact，语法为：compact [string/hash/set/zset/list/all]

```
xxx.qihoo.net:8221> compact
OK
```
一般keyspace比较大时，执行完compact命令后，占用空间会显著减小，但是耗时比较长，对读写性能也有影响，所以建议在流量不大的情况下执行
## readonly （3.1之后版本废除）
该命令用户设置服务器的写权限；执行方式为：
1）“readonly on”  
2）“readonly off”  
3）“readonly 1”   
4）“readonly 0”
其中1）和3）等价，2）和4）等价

```
xxx.qihoo.net:8221> set a b
OK
xxx.qihoo.net:8221> get a
"b"
xxx.qihoo.net:8221> readonly 1
OK
xxx.qihoo.net:8221> set a c
(error) ERR Server in read-only
xxx.qihoo.net:8221> get a
"b"
xxx.qihoo.net:8221> readonly 0
OK
xxx.qihoo.net:8221> set a c
OK
xxx.qihoo.net:8221> get a
"c"
xxx.qihoo.net:8221>
```
