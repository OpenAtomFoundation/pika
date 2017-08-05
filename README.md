# Pika

[![Build Status](https://travis-ci.org/Qihoo360/pika.svg?branch=master)](https://travis-ci.org/Qihoo360/pika)

## Introduction[中文](https://github.com/Qihoo360/pika/blob/master/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization. You can alse use pika in twemproxy or codis(*pika has supported data migration in codis，thanks [left2right](https://github.com/left2right)*) for distributed Redis solution


## UserList

<table>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/dcHpCm4.png" alt="Qihoo"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/ktPV3JU.jpg?2" alt="360game"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/jjZczkN.png" alt="Weibo"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/zoel46r.gif" alt="Garena"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/kHqACbn.png" alt="Apus"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/2c57z8U.png" alt="Ffan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/rUiO5VU.png" alt="Meituan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/px5mEuW.png" alt="XES"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://imgur.com/yJe4FP8.png" alt="HX"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/o8ZDXCH.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://imgur.com/w3qNQ9T.png" alt="GWD"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/Ll6SifR.png" alt="DYD"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/vJbAfri.png" alt="YM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/aNxzwsY.png" alt="XM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/mrWxwkF.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/DX6Ey4p.jpg" alt="LB"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://imgur.com/0oaVKlk.png" alt="YM"></td>
</tr>
</table>

[More](https://github.com/Qihoo360/pika/blob/master/USERS.md)

## Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明) interfaces

## For developer

### RoadMap

* optimize engine nemo to improve list performance

### Dependencies

* snappy - a library for fast data compression
* protobuf - google protobuf library

Upgrade your gcc to version at least 4.8 to get C++11 support.

### Supported platforms

* linux - Centos 5&6

* linux - Ubuntu

If it comes to some missing libs, install them according to the prompts and retry it.

### Compile

Upgrade your gcc to version at least 4.8 to get C++11 support.

Get source code recursive, then pika will pull all submodules

```
git clone git@github.com:Qihoo360/pika.git
```


Then compile pika

```
make
```

## Usage

```
./output/bin/pika -c ./conf/pika.conf
```

## Performance

### test environment

2 same hardware server, one for running pika, the other for running redis-benchmark

	CPU: 24 Cores, Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
	MEM: 165157944 kB
	OS: CentOS release 6.2 (Final)
	NETWORK CARD: Intel Corporation I350 Gigabit Network Connection

### test interfaces
	
	Set, Get
	
### test method

	run pika with 16 work threads, run redis-benchmark on another server as follow: 
	./redis-benchmark -h ... -p ... -n 1000000000 -t set,get -r 10000000000 -c 120 -d 200
	execute 1 billion Set and 1 billion Get commands altogether
	
### test result
``` 
Set
1000000000 requests completed in 11890.80 seconds
18.09% <= 1 milliseconds
93.32% <= 2 milliseconds
99.71% <= 3 milliseconds
99.86% <= 4 milliseconds
99.92% <= 5 milliseconds
99.94% <= 6 milliseconds
99.96% <= 7 milliseconds
99.97% <= 8 milliseconds
99.97% <= 9 milliseconds
99.98% <= 10 milliseconds
99.98% <= 11 milliseconds
99.99% <= 12 milliseconds
...
100.00% <= 19 milliseconds
...
100.00% <= 137 milliseconds

84098.66 requests per second
```
 
```
Get
1000000000 requests completed in 9063.05 seconds
84.97% <= 1 milliseconds
99.76% <= 2 milliseconds
99.99% <= 3 milliseconds
100.00% <= 4 milliseconds
...
100.00% <= 33 milliseconds

110338.10 requests per second
```

### pika vs ssdb ([Detail](https://github.com/Qihoo360/pika/wiki/pika-vs-ssdb))

<img src="http://imgur.com/rGMZmpD.png" height = "400" width = "480" alt="1">
<img src="http://imgur.com/gnwMDof.png" height = "400" width = "480" alt="10">

## pika vs redis
<img src="http://imgur.com/k99VyFN.png" height = "400" width = "600" alt="2">
 
## Documents

1. [Wiki](https://github.com/Qihoo360/pika/wiki)

## Contact Us

Mail: g-infra-bada@360.cn

QQ group: 294254078
For more information about Pika, Atlas and some other technology please pay attention to our Hulk platform official account

<img src="http://i.imgur.com/pL4ni57.png" height = "400" width = "600" alt="2">
