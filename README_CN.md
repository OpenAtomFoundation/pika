# Pika
## 简介 [English](https://github.com/Qihoo360/pika/blob/master/README.md)
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大接口([兼容详情](https://github.com/Qihoo360/pika/wiki/pika支持的redis接口及兼容情况))，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈，并且可以像redis一样，通过slaveof命令进行主从备份，支持全同步和部分同步

## 特点
* 容量大，支持百G数据量的存储
* 兼容redis，不用修改代码即可平滑从redis迁移到pika
* 支持主从(slaveof)
* 完善的[运维](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明)命令

## 编译安装

1.在编译机上安装snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-devel l ibbz2-dev zlib-devel等。CentOS系统可以用yum安装，Ubuntu可以用apt-get安装。如是CentOS系统，执行如下命令：

```
    yum install snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-    devel libbz2-dev zlib-devel
```

2.安装g++(若没有安装), 在CentOS上执行如下命令：

```
    yum install gcc-c++
```

3.把gcc版本临时切换到4.7(若已是，则忽略), 在CentOs上执行如下命令：

```
	a. sudo wget -O /etc/yum.repos.d/slc6-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo
	b. yum install --nogpgcheck devtoolset-1.1
	c. scl enable devetoolset-1.1 bash
```
4.获取源代码

```
	git clone https://github.com/Qihoo360/pika.git && cd pika
```
5.获取依赖的第三方源代码

```
	a. git submodule init
	b. git submodule update
```

6.编译

```
	make __REL=1
```
若编译过程中，提示有依赖的库没有安装，则有提示安装后再重新编译

## 使用
```
	./output/bin/pika -c ./conf/pika.conf
```
若启动失败，把./lib/_VERSION/的内容拷贝到Makefile定义的rpath目录下，然后重新启动

```
	cp PIKA_SOURCE/lib/_VERSION/* RPATH
```
PIKA_SOURCE表示的pika的源代码根目录；
_VERSION表示的是编译机的CenOS版本，如6.2， 5.4...
RPATH在Makefile定义，表示的是程序运行的库预先加载路径

## 性能
```
服务端配置：
	处理器：24核 Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
	内存：165157944 kB
	操作系统：CentOS release 6.2 (Final)
	网卡：Intel Corporation I350 Gigabit Network Connection
客户端配置：
	同服务端
	
测试结果：
	pika配置18个worker，用40个客户端；
	1. 写性能测试：
		方法：客户端依次执行set、hset、lpush、zadd、sadd接口写入数据，每个数据结构10000个key；
		结果：qps 110000
	2. 读性能测试：
		方法：客户端一次执行get、hget、lindex、zscore、smembers，每个数据结构5000000个key；
		结果：qps 170000
```
## 文档
1. [Wiki] (https://github.com/Qihoo360/pika/wiki)

## 联系方式
邮箱：songzhao@360.cn

QQ群：294254078
