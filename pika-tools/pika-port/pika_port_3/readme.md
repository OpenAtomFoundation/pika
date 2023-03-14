## pika_port
---
  **written by AlexStocks(github.com/AlexStocks) 2018/09**

## Intro

Pika-port is like redis-port. it copy a snapshot of pika to pika/redis/codis and then transfer delta data to them.

If u wanna get more details of Pika-port, pls read [Pika笔记](http://alexstocks.github.io/html/pika.html).

This repo just supports Pika v3.0.x.

## Use Case

* [记一次pika迁移到codis](https://blog.csdn.net/wangwenjie2500/article/details/83858572)

## Version list

> V1.6
 * Improvement: add wait-bgsave-timeout to give a warning when pika-port waits bgsave data for a long time
	* Improvement: change rsync configure file's module name from document_${master_ip}:${master_port} to document_${slave_ip}:master_port

> V1.5
	* Improvement: add batch parameter to speed up transfering data between pika and pika/codis

> V1.4
	* Bug Fix: filter out SlotKeyPrefix when sync snapshot data

> V1.3
	* send redis data asynchronously by RedisSender in multiple thread mode

> V1.2
	* send redis data asynchronously by RedisSender

> V1.1
	* filter out SlotKeyPrefix
	* disable ping-pong log

> V1.0
	* Init
