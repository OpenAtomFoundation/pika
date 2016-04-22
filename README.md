#Pika

##Introduction [中文] (https://github.com/baotiao/pika/blob/master/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/baotiao/pika/wiki/pika支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization

##Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/baotiao/pika/wiki/pika的一些管理命令方式说明) interfaces

##Install

```
1. git submodule init && git submodule update
2. make __REL=1 (install snappy,bz2 by youself first)
3. move ./lib/_VERSION/lib/ to the rpath defined in Makefile
```

##Usage

```
./output/bin/pika -c ./conf/pika.conf
```

##Performance

```
Server Info:
	CPU: 24 Cores, Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
	MEM: 165157944 kB
	OS: CentOS release 6.2 (Final)
	NETWORK CARD: Intel Corporation I350 Gigabit Network Connection
Client Info:
	Same as Server

Test:
	Pika run with 18 worker threads, and we test it using 40 client;
	1. Write Performance:
		Client push data by set, hset, lpush, zadd, sadd, each interface has 10000 key range;
		result: 110000 qps
	2. Read Performance:
		Client pull data by get, hget, lindex, zscore, smembers, 25000000 keys stored in pika and each interface has 5000000 key range
		result: 170000 qps

```		
##Documents

1. [Wiki](https://github.com/baotiao/pika/wiki)

##Contact Us

songzhao@360.cn
