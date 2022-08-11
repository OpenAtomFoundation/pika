#### config [get | set | rewrite]
config get、set、rewrite args：

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
It was created by Pika, used to safely purge log manually. 

### client list
Similar to Redis, list all clients information 

### client list order by [addr|idle]
It was created by Pika, list all clients order by ip or connection idle time. 

### client kill all
It was created by Pika, used to kill all connections(include self).

### slowlog
Different from Redis, not only store in memory but also print in error log, should config slowlog-write-errorlog first.

### bgsave
Similiar to Redis bgsave, dump snapshot.

### dumpoff
Force kill the dump(bgsave) job and create a dump-failed forder in dump directory(Deprecated from v2.0)

### delbackup
Delete other snapshots except the running one

### compact
Trigger compact all data structure on rocksdb immediately

### compact [string | hash | set | zset | list ]
Trigger compact in one type of data structure immediately

### compact $db [string | hash | set | zset | list ]
Trigger compact in some db one type of data structure immediately

### flushdb [string | hash | set | zset | list ]
Clear data in one type of data structure

### keys pattern [string | hash | set | zset | list ]
Output keys/pattern in one or some type of data structure, no args means all types

### slaveof ip port [write2file-name] [write2file-pos] [force]
Partial sync from binlog name and offset in master node, force can trigger full sync from master

### pkscanrange type key_start key_end [MATCH pattern] [LIMIT limit]
Scan forward and output keys range in [key_start, key_end]
* type: data structure type，{string_with_value | string | hash| list | zset | set}  
* key_start: start key, empty string is -inf
* key_end: end key, empty string is +inf

### pkrscanrange type key_start key_end [MATCH pattern] [LIMIT limit]
Similar to pkscanrange, reverse scan

### pkhscanrange key field_start field_end  [MATCH pattern] [LIMIT limit]
Scan and list key/value pairs in hash table [field_start, field_end]
* key：hash table key
* field_start： start Field, empty string is -inf
* field_end：end Field, empty string is +inf

### pkhrscanrange key field_start field_end  [MATCH pattern] [LIMIT limit]  
Similar to pkhscanrange, reverse scan