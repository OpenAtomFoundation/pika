#### config [get | set | rewrite]
在服务器配置中，支持参数的get、set、rewrite，支持的参数如下：

-|GET| SET
---|---|---
binlog-file-size	|o|	x
compact-cron	        |o|	o
compact-interval	|o|	o
compression	        |o|	x
daemonize	        |o|	x
db-path	                |o|	x
db-sync-path	        |o|	x
db-sync-speed	        |o|	x
double-master-ip	|o|	o
double-master-port	|o|	x
double-master-sid	|o|	x
dump-expire	        |o|	o
dump-path	        |o|	x
dump-prefix	        |o|	o
expire-logs-days	|o|	o
expire-logs-nums	|o|	o
identify-binlog-type	|o|	o
loglevel	        |o|	o
log-path	        |o|	x
masterauth	        |o|	o
max-background-compactions	|o|	x
max-background-flushes	        |o|	x
max-bytes-for-level-multiplier	|o|	x
max-cache-files	        |o|	x
maxclients              |o|	o
maxmemory	        |o|	x
network-interface	|o|	x
pidfile	                |o|	x
port	                |o|	x
requirepass	        |o|	o
root-connection-num	|o|	o
slaveof	                |o|	x
slave-priority	        |o|	o
slave-read-only	        |o|	o
slotmigrate	        |o(<3.0.0)|o(<3.0.0)
slowlog-log-slower-than	|o|	o
slowlog-write-errorlog  |o(<3.0.2)|o(<3.0.2)
sync-buffer-size	|o|	x
sync-thread-num	        |o|	x
target-file-size-base	|o|	x
thread-num	        |o|	x
timeout      	        |o|	o
userblacklist	        |o|	o
userpass	        |o|	o
write-buffer-size	|o|	x
max-cache-statistic-keys |o(<3.0.6)|o(<3.0.6)
small-compaction-threshold |o(<3.0.6)|o(<3.0.6)
databases               |o(<3.1.0)|x  
write-binlog            |o|     o
thread-pool-size        |o|     x
slowlog-max-len         |o|     o
share-block-cache       |o|     x
optimize-filters-for-hits  |o|  x
level-compaction-dynamic-level-bytes  |o|  x
cache-index-and-filter-blocks |o|  x
block-size              |o|     x
block-cache             |o|     x
sync-window-size        |o|	o



### purgelogsto [write2file-name]
purgelogsto为pika原创命令, 功能为手动清理日志, 类似mysql的purge master logs to命令, 该命令有多重检测机制以确保日志一定为安全清理

### client list
与redis相比, 展示的信息少于redis

### client list order by [addr|idle]
pika原创命令，功能为按照ip address 或者 connection idle时间排序

### client kill all
pika原创命令, 功能为杀死当前所有链接（不包括同步进程但包含自己）

### 慢日志(slowlog)
与redis不同, pika的慢日志不仅存放内存中允许通过slow log命令查看，同时也允许存放在error log中并无条数限制方便接分析，但需要打开slowlog-write-errorlog参数

### bgsave
类似redis的bgsave, 先生成一个快照, 然后再将快照数据进行备份, 备份文件存放在dump目录下

### dumpoff
强行终止正在执行的dump进程(bgsave), 执行该命令后备份会立即停止然后在dump目录下生成一个dump-failed文件夹(Deprecated from v2.0)

### delbackup
删除dump目录下除正在使用（全同步中）的db快照外的其他快照

### compact
立即触发引擎层(rocksdb)所有数据结构执行全量compact操作, 全量compact能够通过sst文件的合并消除已删除或过期但未即时清理的数据, 能够在一定程度上降低数据体积, 需要注意的是, 全量compact会消耗一定io资源

### compact [string | hash | set | zset | list ]
立即触发引擎层(rocksdb)对指定数据结构执行全量compact操作, 指定数据结构的全量compact能够通过sst文件的合并消除已删除或过期但未即时清理的数据, 能够在一定程度上降低该结构数据的数据体积, 需要注意的是, 全量compact会消耗一定io资源

### compact $db [string | hash | set | zset | list ]
对指定的db进行全量compact。例如 compact db0 all会对db0上所有数据结构进行全量compact。

### flushdb [string | hash | set | zset | list ]
flushdb命令允许只清除指定数据结构的所有数据, 如需删除所有数据请使用flushall

### keys pattern [string | hash | set | zset | list ]
keys命令允许只输出指定数据结构的所有key, 如需输出所有结构的key请不要使用参数

### slaveof ip port [write2file-name] [write2file-pos] [force]
slaveof命令允许通过指定write2file(binlog)的文件名称及同步位置来实现增量同步, force参数用于触发强行全量同步(适用于主库write2file被清理无法为从库提供增量同步的场景), 全量同步后pika会自动切换至增量同步

### pkscanrange type key_start key_end [MATCH pattern] [LIMIT limit]
对指定数据结构进行正向scan, 列出处于区间 [key_start, key_end] 的Key列表(如果type为string_with_value，则列出的是key-value列表) ("", ""] 表示整个区间。
* type： 指定需要scan数据结构的类型，{string_with_value | string | hash| list | zset | set}  
* key_start： 返回的起始Key, 空字符串表示 -inf(无限小)  
* key_end：返回的结束Key, 空字符串表示 +inf(无限大)  

### pkrscanrange type key_start key_end [MATCH pattern] [LIMIT limit]
类似于pkscanrange, 逆序

### pkhscanrange key field_start field_end  [MATCH pattern] [LIMIT limit]
列出指定hash table中处于区间 [field_start, field_end] 的 field-value 列表.
* key：hash table对应的key
* field_start： 返回的起始Field, 空字符串表示 -inf(无限小)
* field_end：返回的结束Field, 空字符串表示 +inf(无限大)

### pkhrscanrange key field_start field_end  [MATCH pattern] [LIMIT limit]  
类似于pkhscanrange, 逆序

### diskrecovery 
Pika 原创命令，功能为当磁盘意外写满后，RocksDB 会进入写保护状态，当我们将空间调整为充足空间时，这个命令可以将 RocksDB 的写保护状态解除，变为可以继续写的状态, 避免了 Pika 因为磁盘写满后需要重启才能恢复写的情况，执行成功时返回 OK，如果当前磁盘空间依然不足，执行这个命令返回`"The available disk capacity is insufficient`，该命令执行时不需要额外参数，只需要执行 diskrecovery 即可。