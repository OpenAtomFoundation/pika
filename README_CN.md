# Pika
## 简介 [English](https://github.com/baotiao/pika/blob/master/README.md)
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大接口([兼容详情](https://github.com/baotiao/pika/wiki/pika支持的redis接口及兼容情况))，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈，并且可以像redis一样，通过slaveof命令进行主从备份，支持全同步和部分同步

## 特点
* 容量大，支持百G数据量的存储
* 兼容redis，不用修改代码即可平滑从redis迁移到pika
* 支持主从(slaveof)
* 完善的[运维](https://github.com/baotiao/pika/wiki/pika的一些管理命令方式说明)命令

## 编译安装
```
1. git submodule init && git submodule update
2. make __REL=1 (编译依赖的某些库如snappy，bz2请自行提前安装）
3. 将./lib/_VERSION_/lib目录移动到Makefile中自定义的rpath路径，pika启动时会从rpath加载相关so
```

## 使用
```
./output/bin/pika -c ./conf/pika.conf
```

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
1. [Wiki] (https://github.com/baotiao/pika/wiki)

## 联系方式
songzhao@360.cn
