## pika_port
**pika port is like redis-port. it copy a snapshot of pika to pika/redis/codis and then transfer delta data to them.**

## Version list

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
