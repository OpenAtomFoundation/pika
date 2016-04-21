# Pika
## 简介 [English](https://github.com/baotiao/pika/tree/pika2.0)
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大接口，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈，并且可以像redis一样，通过slaveof命令进行主从备份，支持全同步和部分同步

## 特点
* 容量大，支持百G数据量的存储
* 兼容redis，不用修改代码即可平滑从redis迁移到pika
* 支持主从

## 编译安装
```
git submodule update
make __REL=1
```

## 使用
```
./output/bin/pika -c ./conf/pika.conf
```

## 性能

## 文档

## 联系方式