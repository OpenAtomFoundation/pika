pika当前支持的redis接口
pika支持redis五种类型（分别为string、hash、list、set、zset）的接口，先列出其对redis的五种数据结构兼容统计。  

#### 统计所用的标记含义如下：

| 图标        |    含义                               |
| :--------:  | :--------:                           | 
| o           | 该接口完全支持，使用方式与redis没有任何区别   |
| ！          |	功能支持，但使用或输出与redis有部分差异，需注意|
|×            |当前还未支持                                |

---

## Keys
|接口|DEL|DUMP|EXISTS|EXPIRE|EXPIREAT|KEYS|MIGRATE|MOVE|OBJECT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|x|o|o|o|o|x|x|x|
|接口|PERSIST|PEXPIRE|PEXPIREAT|PTTL|RANDOMKEY|RENAME|RENAMENX|RESTORE|SORT|
|状态|o|!|!|o|x|x|x|x|x|
|接口|TOUCH|TTL|TYPE|UNLINK|WAIT|SCAN|
|状态|x|o|!|x|x|!|

**备注:**  

* PEXPIRE：无法精确到毫秒，底层会自动截断按秒级别进行处理;  

* PEXPIREAT：无法精确到毫秒，底层会自动截断按秒级别进行处理; 
* SCAN：会顺序迭代当前db的快照，由于pika允许重名五次，所以scan有优先输出顺序，依次为：string -> hash -> list -> zset -> set;  
* TYPE：另外由于pika允许重名五次，所以type有优先输出顺序，依次为：string -> hash -> list -> zset -> set，如果这个key在string中存在，那么只输出sting，如果不存在，那么则输出hash的，依次类推。
* KEYS: KEYS命令支持参数支持扫描指定类型的数据，用法如 "keys * [string, hash, list, zset, set]"

## Strings

|接口|APPEND|BITCOUNT|BITFIELD|BITOP|BITPOS|DECR|DECRBY|GET|GETBIT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|x|!|o|o|o|o|!|
|接口|GETRANGE|GETSET|INCR|INCRBY|INCRBYFLOAT|MGET|MSET|MSETNX|STRLEN|
|状态|o|o|o|o|o|o|o|o|o|o|
|接口|PSETEX|SET|SETBIT|SETEX|SETNX|SETRANGE|
|状态|o|o|!|o|o|o|


**备注:**  

* BIT操作：与Redis不同，Pika的bit操作范围为2^21， bitmap的最大值为256Kb。redis setbit 只是对key的value值更新。但是pika使用rocksdb作为存储引擎，rocksdb只会新写入数据并且只在compact的时候才从硬盘删除旧数据。如果pika的bit操作范围和redis一致都是2^32的话，那么有可能每次对同一个key setbit时，rocksdb都会存储一个512M大小的value。这会产生 严重的性能隐患。因此我们对pika的bit操作范围作了取舍。

## Hashes

|接口|HDEL|HEXISTS|HGET|HGETALL|HINCRBY|HINCRBYFLOAT|HKEYS|HLEN|HMGET|HMSET|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|o|o|o|o|
|接口|HSET|HSETNX|HVALS|HSCAN|HSTRLEN|
|状态|!|o|o|o|o|

**备注:**
* HSET操作：暂不支持单条命令设置多个field value，如有需求请用HMSET

## Lists

|接口|LINDEX|LINSERT|LLEN|LPOP|LPUSH|LPUSHX|LRANGE|LREM|LSET|LTRIM|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|o|o|o|o|
|接口|RPOP|RPOPLPUSH|RPUSH|RPUSHX|BLPOP|BRPOP|BRPOPLPUSH|
|状态|o|o|o|o|x|x|x|

## Sets

|接口|SADD|SCARD|SDIFF|SDIFFSTORE|SINTER|SINTERSTORE|SISMEMBER|SMEMBERS|SMOVE|SPOP|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|o|o|o|o|
|接口|SRANDMEMBER|SREM|SUNION|SUNIONSTORE|SSCAN|
|状态|!|o|o|o|o|

**备注：**  

* SRANDMEMBER：时间复杂度O( n )，耗时较多

## Sorted Sets

|接口|ZADD|ZCARD|ZCOUNT|ZINCRBY|ZRANGE|ZRANGEBYSCORE|ZRANK|ZREM|ZREMRANGEBYRANK|ZREMRANGEBYSCORE|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|o|o|o|o|
|接口|ZREVRANGE|ZREVRANGEBYSCORE|ZREVRANK|ZSCORE|ZUNIONSTORE|ZINTERSTORE|ZSCAN|ZRANGEBYLEX|ZLEXCOUNT|ZREMRANGEBYLEX|
|状态|o|o|o|o|o|o|o|o|o|o|

* ZADD 的选项 [NX|XX] [CH] [INCR] 暂不支持

## HyperLogLog

|接口|PFADD|PFCOUNT|PFMERGE|
|:-:|:-:|:-:|:-:|
|状态|o|o|o|

**备注：**

* 50w以内误差均小于1%, 100w以内误差小于3%, 但付出了时间代价.

## GEO

|接口|GEOADD|GEODIST|GEOHASH|GEOPOS|GEORADIUS|GEORADIUSBYMEMBER|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|


## Pub/Sub

|接口|PSUBSCRIBE|PUBSUB|PUBLISH|PUNSUBSCRIBE|SUBSCRIBE|UNSUBSCRIBE|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|o|o|o|o|o|o|

**备注：**

* 暂不支持keyspace notifications

## 管理命令（这里仅列出pika兼容的）

|接口|INFO|CONFIG|CLIENT|PING|BGSAVE|SHUTDOWN|SELECT|
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|状态|!|o|!|o|o|o|!|

**备注：**  

* info：info支持全部输出，也支持匹配形式的输出，例如可以通过info stats查看状态信息，需要注意的是key space与redis不同，pika对于key space的展示选择了分类型展示而非redis的分库展示（因为pika没有库），pika对于key space的统计是被动的，需要手动触发，然后pika会在后台进行统计，pika的key space统计是精确的。触发方式为执行：keyspace命令即可，然后pika会在后台统计，此时可以使用：keyspace readonly命令来进行查看，readonly参数可以避免反复进行统计，如果当前数据为0，则证明还在统计中；  

* client：当前client命令支持client list及client kill，client list显示的内容少于redis；  

* select：该命令在3.1.0版前无任何效果，自3.1.0版开始与Redis一致;

* ping: 该命令仅支持无参数使用，即使用`PING`，客户端返回`PONG`.
---

# Pika Pub/Sub文档

可用版本： >= 2.3.0

注意:	暂不支持键空间通知功能


## Pika 发布订阅命令
##### 以下为Pub/Sub发布订阅命令, 与Redis完全兼容

* PUBSUB subcommand [argument [argument ...]]
* PUBLISH channel message
* SUBSCRIBE channel [channel ...]
* PSUBSCRIBE pattern [pattern ...]
* UNSUBSCRIBE [channel [channel ...]]
* PUNSUBSCRIBE [pattern [pattern ...]]

#### 具体使用方法参考Redis的[Pub/Sub文档](http://redisdoc.com/topic/pubsub.html)


## 重要说明  

* 重名问题：由于pika每个类型独立运作， 所以允许重名。例如在key abc在string中存在的时候也同样允许在hash中存在，一个key最多重名5次（5大类型），但在同一接口中是无法重名的。所以建议在使用的时候对于不同类型不要使用完全相同的key;    

* 分库问题：pika自3.1.0版起支持多库，相关命令、参数的变化请参考[Pika3.1.0多库版命令、参数变化参考文档](multiDB.md)

* 数据展示：pika对于keyspace的展示选择了分类型展示而非redis的分库展示（因为pika没有分库概念），pika对于keyspace的统计是被动的，需要手动触发并不会立即输出，命令为：info keyspace [ 0|1 ]，默认为0不触发，pika的keyspace统计是精确的。

* Pika 的 ZRemrangebyscore 命令采用 RocksDB 的 DeleteRange 方法，不再迭代删除元素，命令现在不返回删除的个数，返回 DeleteRange 的执行状态，成功时返回 0. 
