#Pika

[![Build Status](https://travis-ci.org/Qihoo360/pika.svg?branch=master)](https://travis-ci.org/Qihoo360/pika)

##Introduction [中文] (https://github.com/Qihoo360/pika/blob/master/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization

##Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明) interfaces


## Quickstart and Try
  You can try to use our pre-build binary versions. For now, only Centos5 and Centos6 are supported. The binary ones can be found at [the release page](https://github.com/Qihoo360/pika/releases) which are called pikaX.Y.Z_centosK_bin.tar.gz.

```
# 1. unzip file
tar zxf pikaX.Y.Z_centosK_bin.tar.gz
# 2. change working directory to output
#     note: we should in this directory, caz the RPATH is ./lib;
cd output
# 3. run pika:
./bin/pika -c conf/pika.conf
```

## Install


### Dependencies

* snappy - a library for fast data compression
* zlib - a library for fast data compression
* bzips - a library for fast data compression
* protobuf - google protobuf library
* glog - google log library

### Supported platforms

* linux - Centos 5&6

* linux - Ubuntu

If it comes to some missing libs, install them according to the prompts and retry it.

### Compile

Upgrade your gcc to version at least 4.7 to get C++11 support.

Then just type 

```
make __REL=1
```

## Usage

```
	./output/bin/pika -c ./conf/pika.conf
```

If failed, move pika source/lib/_VERSION/lib/ to the rpath defined in Makefile and relanch. 

~~~
	cp PIKA_SOURCE/lib/_VERSION/* RPATH
~~~
The PIKA_SOURCE stands for pika source code's root directory;  
The __VERSION represents the OS's version, such as 6.2, 5.4...  
The RPATH is defined in pika's Makefile


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

##UserList

<img src="http://i.imgur.com/dcHpCm4.png" height = "100" width = "120" alt="Qihoo">
<img src="http://i.imgur.com/jjZczkN.png" height = "100" width = "120" alt="Weibo">
<img src="http://imgur.com/a/DPAJ3" height = "100" width = "120" alt="Garena">
<img src="http://i.imgur.com/kHqACbn.png" height = "100" width = "120" alt="Apus">
<img src="http://i.imgur.com/2c57z8U.png" height = "100" width = "120" alt="Ffan">
<img src="http://i.imgur.com/2c57z8U.png" height = "100" width = "120" alt="Ffan">
<img src="http://imgur.com/a/oeyNQ" height = "100" width = "120" alt="Meituan">

[More](https://github.com/Qihoo360/pika/blob/master/USERS.md)

 
##Documents

1. [Wiki](https://github.com/Qihoo360/pika/wiki)

##Contact Us

Mail: g-infra-bada@360.cn

QQ group: 294254078
