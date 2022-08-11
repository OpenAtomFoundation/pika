## client kill all
Kill all clients

```
xxx.qihoo.net:8221> client kill all
OK
xxx.qihoo.net:8221>
```

## bgsave
Same as Redis, but in asynchronized way, data dump to 'dump_path', include prefix information

```
xxx.qihoo.net:8221> BGSAVE
20160422134755 : 2213: 32582292
```
Response information, include binlog file number and offset
    
```
xxx.qihoo.net # ll /data3/pika_test/dump/
zize 0
drwxr-xr-x 1 root root 42 4月  22 13:47 pika8221-20160422
```
"/data3/pika_test/dump/" is dump_path，"pika9221-" is dump_prefix，20160422 is the date of this dump

## delbackup
delete other database snapshot except current running in the dump path

```
xxx.qihoo.net:8221> DELBACKUP
OK
```

## info keyspace
Can type “info keyspace 1”,“info keyspace 0” and ”info keyspace“. “info keyspace” is same as “info keyspace 0”  
info keyspace 1： start a keyspace scan asynchronize 
info keyspace 0: return last keyspace scan relust immediately

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
"config get" and "config set" are all same with Redis, but options are different 
1) config get *  
2) config set *

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
Force compact on rocksdb manually. Can support data structure dimension like: compact [string/hash/set/zset/list/all]

```
xxx.qihoo.net:8221> compact
OK
```
Avoid compact in high traffic, because it is impact on performance.
## readonly (retired after v3.1）
1）“readonly on”  
2）“readonly off”  
3）“readonly 1”   
4）“readonly 0”
1）is same to 3, 2）is same to 4)

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
