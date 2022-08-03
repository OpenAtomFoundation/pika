### 概述

Pika 一款开源的高性能持久化的NoSQL产品，兼容Redis协议，数据持久化存储到RocksDB存储引擎，具有两种运行模式： 经典模式（Classic） & 分布式模式（Sharding）。
* 经典模式（Classic）： 即1主N从同步模式，1个主实例存储所有的数据，N个从实例完全镜像同步主实例的数据，每个实例支持多个DBs。DB默认从0开始，Pika的配置项databases可以设置最大DB数量。DB在Pika上的物理存在形式是一个文件目录。
* 分布式模式（Sharding）： Sharding模式下，将用户存储的数据集合称为Table，每个Table切分成多个分片，每个分片称为Slot，对于某一个KEY的数据由哈希算法计算决定属于哪个Slot。将所有Slots及其副本按照一定策略分散到所有的Pika实例中，每个Pika实例有一部分主Slot和一部分从Slot。在Sharding模式下，分主从的是Slot而不再是Pika实例。Slot在Pika上的物理存在形式是一个文件目录。

Pika可以通过配置文件中的instance-mode配置项，设置为classic和sharding，来选择运行经典模式（Classic）还是分布式模式（Sharding）的Pika。

### 1. 经典（主从）模式

![](https://raw.githubusercontent.com/simpcl/simpcl.github.io/master/PikaClassic.png)


### 2. 分布式模式

![](https://raw.githubusercontent.com/simpcl/simpcl.github.io/master/PikaCluster.png)