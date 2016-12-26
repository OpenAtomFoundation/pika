# Pika
## 简介 [English](https://github.com/Qihoo360/pika/blob/master/README.md)
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大接口([兼容详情](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况))，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈，并且可以像redis一样，通过slaveof命令进行主从备份，支持全同步和部分同步，pika还可以用在twemproxy或者codis中来实现静态数据分片（pika已经可以支持codis的动态迁移slot功能，目前在pika的codis分支中持续完善中，欢迎试用，感谢作者[left2right](https://github.com/left2right)同学提交pr）
##Pika用户

<img src="http://i.imgur.com/dcHpCm4.png" height = "100" width = "120" alt="Qihoo">
<img src="http://i.imgur.com/jjZczkN.png" height = "100" width = "120" alt="Weibo">
<img src="http://i.imgur.com/zoel46r.gif" height = "100" width = "120" alt="Garena">
<img src="http://i.imgur.com/kHqACbn.png" height = "100" width = "120" alt="Apus">
<img src="http://i.imgur.com/2c57z8U.png" height = "100" width = "120" alt="Ffan">

<img src="http://i.imgur.com/rUiO5VU.png" height = "100" width = "120" alt="Meituan">
<img src="http://i.imgur.com/px5mEuW.png" height = "100" width = "120" alt="XES">
<img src="http://imgur.com/yJe4FP8.png" height = "100" width = "120" alt="HX">
<img src="http://i.imgur.com/o8ZDXCH.png" height = "100" width = "120" alt="XL">
<img src="http://imgur.com/w3qNQ9T.png" height = "100" width = "120" alt="GWD">


[更多](https://github.com/Qihoo360/pika/blob/master/USERS.md)

## 特点
* 容量大，支持百G数据量的存储
* 兼容redis，不用修改代码即可平滑从redis迁移到pika
* 支持主从(slaveof)
* 完善的[运维](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明)命令


## 快速试用
  如果想快速试用pika，目前提供了Centos5，Centos6的binary版本，可以在[release页面](https://github.com/Qihoo360/pika/releases)看到，具体文件是pikaX.Y.Z_centosK_bin.tar.gz。

```
# 1. 解压文件
tar zxf pikaX.Y.Z_centosK_bin.tar.gz
# 2. 切到output目录（rpath是./lib）
cd output
# 3. 运行pika:
./bin/pika -c conf/pika.conf
```

## 编译安装

1.在编译机上安装snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-devel l ibbz2-dev zlib-devel等。CentOS系统可以用yum安装，Ubuntu可以用apt-get安装。如是CentOS系统，执行如下命令：

```
    yum install snappy-devel bz2 libzip-dev libsnappy-dev libprotobuf-dev libevent-dev protobuf-compiler libgoogle-glog-dev protobuf-devel libevent-devel bzip2-    devel libbz2-dev zlib-devel
```

2.安装g++(若没有安装), 在CentOS上执行如下命令：

```
    yum install gcc-c++
```

3.把gcc版本临时切换到4.8(若已是，则忽略), 在CentOs上执行如下命令：

```
	a. sudo rpm --import http://ftp.scientificlinux.org/linux/scientific/5x/x86_64/RPM-GPG-KEYs/RPM-GPG-KEY-cern
	b. sudo wget http://people.centos.org/tru/devtools-2/devtools-2.repo -O /etc/yum.repos.d/devtools-2.repo
	c. sudo yum install -y devtoolset-2-gcc devtoolset-2-binutils devtoolset-2-gcc-c++
	d. scl enable devtoolset-2 bash
```
4.获取源代码

```
	git clone --recursive https://github.com/Qihoo360/pika.git && cd pika
```

5.编译

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

###测试环境：
```
	相同配置服务端、客户机各一台：
	处理器：24核 Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
	内存：165157944 kB
	操作系统：CentOS release 6.2 (Final)
	网卡：Intel Corporation I350 Gigabit Network Connection
```
###测试接口：
```
	Set、Get
```

###测试方法：
```
	pika配16个worker，客户机执行 ./redis-benchmark -h ... -p ... -n 1000000000 -t set,get -r 10000000000 -c 120 -d 200
	通过set和get接口对pika进行10亿次写入+10亿次读取
```

###测试结果：
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

###与SSDB性能对比（[详情](https://github.com/Qihoo360/pika/wiki/pika-vs-ssdb)）
<img src="http://imgur.com/rGMZmpD.png" height = "400" width = "480" alt="1">
<img src="http://imgur.com/gnwMDof.png" height = "400" width = "480" alt="10">

###与Redis性能对比
<img src="http://imgur.com/k99VyFN.png" height = "400" width = "600" alt="2">

## 文档
1. [Wiki] (https://github.com/Qihoo360/pika/wiki)

## 联系方式
邮箱：g-infra-bada@360.cn

QQ群：294254078
