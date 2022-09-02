## What is Pika
Pika is Nosql(Key/Value) databases, well known as persistency KV storage(presistency Redis). Pika support Redis protocol, users can use any Redis clients send Redis commands to Pika. User can also migrate business data from Redis to Pika and use Pika instead of Redis without change any code. Pika can provide high volume and persistency storate service with most data struecture/commands in Redis(ops/API.md). Pika also have master/slave replication mode for high available. Migration tools from Redis to Pika is supported by Pika team at the same time, smooth and seamless.
## Compare with Redis
Compare with Redis, the outstanding achievement is persistency storage

### Pro：
1. High Capacity：Disk size storage is far more than memory size.
2. Fast Recovery：No need waiting a long time to load rdb or aof when restart.
3. Fast Backup：Rsync backup files is fast.

### Cons：
Low performance when access data in disks, which can use SSD disk to improve.

## Scene
If your business data is too large enough that Redis cannot store, or your business data is very important can not tolerate lost, Pika is your choice.

## Features
1. Hith volume, persistency storage
2. Redis protocol compatible
3. Support master-slave replication mode, high available 
4. Useful admin and operation commands support

## Situations in 360.com
More than 20 large clusters. 
More than 10 billion requests per day. 
More than 3T data store.

## Performance 
### Configuration
- CPU: 24 Cores, Intel® Xeon® CPU E5-2630 v2 @ 2.60GHz
- MEM: 165157944 kB
- OS: CentOS release 6.2 (Final)
- NETWORK CARD: Intel Corporation I350 Gigabit Network Connection

### Steps
Pika:150G data write to Pika which is Hash key 50 and fields more than 10 million with threads 18
Redis: single thread
 ![](images/benchmarkVsRedis01.jpeg)

### Conclution
Compare with one Redis instance performance, Pika is low because of the multiple thread design, but if run in multiple core machine with some data structure, it's performance is higher.


## Others
### Pika vs SSDB ([Detail](pikaVsSSDB.md))

<img src="images/benchmarkVsSSDB01.png" height = "400" width = "480" alt="1">

<img src="images/benchmarkVsSSDB02.png" height = "400" width = "480" alt="10">

## Pika vs Redis
<img src="images/benchmarkVsRedis02.png" height = "400" width = "600" alt="2">

## How to migrate 
### Development
None
### OPS
1. Data full synchronize
1. Data partial synchronize
1. Load balancer change

 
 

 
 