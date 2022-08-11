Pika support 5 types of Redis data structure: string,hash,list,set,zset  

#### label

| label        | meaning                                  |
| :--------:  | :--------:                           | 
| o           | full support   |
| ！          |	partial support|
|×            | not support                                |

---

## Keys
|Command|DEL|DUMP|EXISTS|EXPIRE|EXPIREAT|KEYS|MIGRATE|MOVE|OBJECT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|x|o|o|o|o|x|x|x|
|Command|PERSIST|PEXPIRE|PEXPIREAT|PTTL|RANDOMKEY|RENAME|RENAMENX|RESTORE|SORT|
|Status|o|!|!|o|x|x|x|x|x|
|Command|TOUCH|TTL|TYPE|UNLINK|WAIT|SCAN|
|Status|x|o|!|x|x|!|

**Tips**  

* PEXPIRE: millisecond not support
* PEXPIREAT: millisecond not support
* SCAN: scan every database, keys maybe duplicate(max 5 times) in every database but in the order of string -> hash -> list -> zset -> set
* TYPE: keys maybe duplicate(max 5 times), so the output order is: string -> hash -> list -> zset -> set
* KEYS: this command support output scan some type like: "keys * [string, hash, list, zset, set]"

## Strings

|Command|APPEND|BITCOUNT|BITFIELD|BITOP|BITPOS|DECR|DECRBY|GET|GETBIT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|x|!|o|o|o|o|!|
|Command|GETRANGE|GETSET|INCR|INCRBY|INCRBYFLOAT|MGET|MSET|MSETNX|STRLEN|
|Status|o|o|o|o|o|o|o|o|o|o|
|Command|PSETEX|SET|SETBIT|SETEX|SETNX|SETRANGE|
|Status|o|o|!|o|o|o|


**Tips**  

* BIT：Different with Redis, range in Pika is 2^21, max is 256Kb

## Hashes

|Command|HDEL|HEXISTS|HGET|HGETALL|HINCRBY|HINCRBYFLOAT|HKEYS|HLEN|HMGET|HMSET|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|o|o|o|o|
|Command|HSET|HSETNX|HVALS|HSCAN|HSTRLEN|
|Status|!|o|o|o|o|

**Tips**
* HSET: not support set multiple field/value, can use HMSET

## Lists

|Command|LINDEX|LINSERT|LLEN|LPOP|LPUSH|LPUSHX|LRANGE|LREM|LSET|LTRIM|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|o|o|o|o|
|Command|RPOP|RPOPLPUSH|RPUSH|RPUSHX|BLPOP|BRPOP|BRPOPLPUSH|
|Status|o|o|o|o|x|x|x|

## Sets

|Command|SADD|SCARD|SDIFF|SDIFFSTORE|SINTER|SINTERSTORE|SISMEMBER|SMEMBERS|SMOVE|SPOP|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|o|o|o|o|
|Command|SRANDMEMBER|SREM|SUNION|SUNIONSTORE|SSCAN|
|Status|!|o|o|o|o|

**Tips**  

* SRANDMEMBER: O(n)

## Sorted Sets

|Command|ZADD|ZCARD|ZCOUNT|ZINCRBY|ZRANGE|ZRANGEBYSCORE|ZRANK|ZREM|ZREMRANGEBYRANK|ZREMRANGEBYSCORE|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|o|o|o|o|
|Command|ZREVRANGE|ZREVRANGEBYSCORE|ZREVRANK|ZSCORE|ZUNIONSTORE|ZINTERSTORE|ZSCAN|ZRANGEBYLEX|ZLEXCOUNT|ZREMRANGEBYLEX|
|Status|o|o|o|o|o|o|o|o|o|o|

* ZADD [NX|XX] [CH] [INCR] not support

## HyperLogLog

|Command|PFADD|PFCOUNT|PFMERGE|
|:-:|:-:|:-:|:-:|
|Status|o|o|o|

**Tips**

* Precision less than 1% if count less than 500k
* Precision less than 3% if count less than 1m

## GEO

|Command|GEOADD|GEODIST|GEOHASH|GEOPOS|GEORADIUS|GEORADIUSBYMEMBER|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|


## Pub/Sub

|Command|PSUBSCRIBE|PUBSUB|PUBLISH|PUNSUBSCRIBE|SUBSCRIBE|UNSUBSCRIBE|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|o|o|o|o|o|o|

**Tips**

* keyspace notifications not support

## Admin Command(Pika compatible)

|Command|INFO|CONFIG|CLIENT|PING|BGSAVE|SHUTDOWN|SELECT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|Status|!|o|!|o|o|o|!|

**Tips:**  

* info: different with Redis in key space. Should input keyspace first, should wait for the result until this job finished asynchronize 

* client: support client list,client kill,client list, but output less than Redis

* select: not support before v3.1.0, same as Redis after v3.1.0

* ping: only support no args like `PING`，response `PONG`.
---

# Pika Pub/Sub

 version >= 2.3.0

 tips: not support key space notification


## Pika Pub/Sub
##### Same with Redis below

* PUBSUB subcommand [argument [argument ...]]
* PUBLISH channel message
* SUBSCRIBE channel [channel ...]
* PSUBSCRIBE pattern [pattern ...]
* UNSUBSCRIBE [channel [channel ...]]
* PUNSUBSCRIBE [pattern [pattern ...]]

#### refer to [Pub/Sub](http://redisdoc.com/topic/pubsub.html)


## Important 

* Key Duplicate: Every data structure in Pika is run independently, so at most 5 same key names.  

* Database: From Pika 3.1.0, we support multiple DB can refer to [Pika3.1.0 multiple database](multiDB.md)

* Data Statistic: Asynchronize job to scan in Pika: info keyspace [ 0|1 ], 0 is default
  
