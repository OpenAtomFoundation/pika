# pika-port
---
*Pika-port is used to move data from pika to pika/redis. This repo has been [included officially](https://github.com/Qihoo360/pika/wiki/pika%E5%88%B0redis%E8%BF%81%E7%A7%BB%E5%B7%A5%E5%85%B7).*

## Intro
---

Pika-port is like redis-port. it copy a snapshot of pika to pika/redis/codis and then transfer delta data to them.
If u wanna get more details of Pika-port, pls read [Pika笔记](http://alexstocks.github.io/html/pika.html).

## Chinese Intro
---

Pika-port 把自已伪装成 pika 的 slave, 通过 trysync 先把数据全量同步给 pika/redis，然后再进行增量通过。

大致流程：

>- 1 发送 trysync 全量同步命令 -> 接收 dump 数据 -> 解析 dump 数据 -> 过滤 -> 发送给 pika/redis；
>- 2 发送 trysync 增量同步命令 -> 接收 binlog -> 解析 binlog 数据 -> 过滤 -> 发送给 pika/redis；

## Use Case
---

- [记一次pika迁移到codis](https://blog.csdn.net/wangwenjie2500/article/details/83858572)

## Compilation
---

### pika port for pika 2.3.x
sh build2.sh


### pika port for pika 3.0.x
sh build3.sh
