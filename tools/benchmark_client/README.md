## 参数
所有的命令行参数如下所示：
```
  -command (command to execute, eg: generate/get/set/zadd) type: string default: "generate"
  -compare_value (whether compare result or not) type: bool default: false
  -count (request counts per thread) type: int32 default: 100000
  -dbs (dbs name, eg: 0,1,2) type: string default: "0"
  -element_count (elements number in hash/list/set/zset) type: int32 default: 1
  -host (target server's host) type: string default: "127.0.0.1"
  -key_size (key size int bytes) type: int32 default: 50
  -password (password) type: string default: ""
  -port (target server's listen port) type: int32 default: 9221
  -thread_num (concurrent thread num) type: int32 default: 10
  -timeout (request timeout) type: int32 default: 1000
  -value_size (value size in bytes) type: int32 default: 100
```

compare_value: 是否进行数据校验。如需进行数据校验，set和set操作都需要设置为true。如果compare_value为true，执行set命令时的value值即为key值拼接得到，如果哦compare_value为false，value为随机值。

count: 每个线程请求的key个数。

element_count: list/zset/set/hash 每个pkey下的member个数。

目前支持的command包括：get,set,hset,hgetall,sadd,smembers,lpush,lrange,zadd,zrange

## 使用方式
需要先执行generate方式生成待请求的key，如：
```
./benchmark_client --command=generate --count=2 --element_count=10 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --compare_value=1
```
执行完成后会在当前目录生成两个benchmark_keyfile_*文件（每个线程生成一个），每个文件行数为20（element_count * count），每个key长度为10.

接下来可以执行redis API命令，命令执行时，会首先从benchmark_keyfile_*文件中读取到key，在根据compare_value值设置value值。

set命令：
```
./benchmark_client --command=set --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --compare_value=1
```

get命令：
```
./benchmark_client --command=get --count=2 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --compare_value=1
```

hset命令：
```
//将向pika写入共4个hash pkey，每个pkey包含10个member。
./benchmark_client --command=set --count=2 --element_count=10 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --compare_value=1
```

hgetall命令：
```
./benchmark_client --command=hgetall --count=2 --element_count=10 --port=9271 --thread_num=2 --key_size=10 --value_size=25 --host=127.0.0.1 --compare_value=1
```

## 执行结果
```
=================== Benchmark Client ===================
Server host name: 127.0.0.1
Server port: 9271
command: set
Thread num : 2
Payload size : 25
Number of request : 2
Transmit mode: No Pipeline
Collection of dbs: 0
Elements num: 1
CompareValue : 1
Startup Time : Tue Dec 19 20:21:44 2023

========================================================
Table 0 Thread 0 Select DB Success, start to write data...
Table 0 Thread 1 Select DB Success, start to write data...
finish 0 request
finish 0 request
Finish Time : Tue Dec 19 20:21:44 2023
Total Time Cost : 0 hours 0 minutes 0 seconds
Timeout Count: 0 Error Count: 0
stats: Count: 4 Average: 166.5000  StdDev: 24.64
Min: 0  Median: 202.0000  Max: 202
Percentiles: P50: 202.00 P75: 202.00 P99: 202.00 P99.9: 202.00 P99.99: 202.00
------------------------------------------------------

```
包括两部分，第一部分是本次benchmark描述信息。第二部分是统计信息，包括超时请求个数，错误请求个数，请求耗时的统计信息。

