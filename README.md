#Pika

##Introduction [中文] (https://github.com/Qihoo360/pika/blob/master/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/Qihoo360/pika/wiki/pika支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization

##Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明) interfaces

##Install

1.Install snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-devel libbz2-dev zlib-devel etc on you compiling host(if alreadly installed, ignore it); using "yum install" on centos system("apt-get install" on ubuntu system) is ok. If on CentOS system, run the following commands:
   
~~~
	 yum install snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-devel libbz2-dev zlib-devel
~~~
2.Install g++(if installed, skip). Similarly, just using "yum install" on the CentOS(apt-get on Ubuntu) is ok:
 
~~~
	yum install gcc-c++
~~~
3.If your gcc's version is below 4.7, then change it to the 4.7 temporary. For CentOS system, run the flowing commands:

~~~  
	a. sudo wget -O /etc/yum.repos.d/slc6-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo
	b. yum install --nogpgcheck devtoolset-1.1
	c. scl enable devetoolset-1.1 bash
~~~
4.Fetch the source code: 

~~~
	a. git clone git@github.com:Qihoo360/pika
	b. cd pika
~~~

5.Get the third party dependencies:

~~~ 
	a. git submodule init
	b. git submodule update
~~~
6.Compile: 

~~~
	make __REL=1
~~~
If it comes to some missing libs, install them according to the prompts and retry it.

##Usage

~~~
	./output/bin/pika -c ./conf/pika.conf
~~~
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
##Documents

1. [Wiki](https://github.com/Qihoo360/pika/wiki)

##Contact Us

songzhao@360.cn
