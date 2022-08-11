```
# port
port : 9221

# thread number, had better less or equal with the count of CPU core
thread-num : 1

# thread pool size for handle requests
thread-pool-size : 8

# thread num of slave handle sync from master
sync-thread-num : 6

# task queue size of each thread for handle sync from masger
sync-buffer-size : 10

# log store directory for service log and binlog(write2fine)
log-path : ./log/

# data store directory
db-path : ./db/

# size of rocksdb memtable
[RocksDb-Tuning-Guide](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
write-buffer-size : 268435456

# connection idle timeout in seconds, default is 60s
timeout : 60

# admin password, if empty then same to 'userpass', if same as 'userpass' then all user are admin and no limit from userblacklist
requirepass : password

# sync from master auth password like Redis
masterauth :

# user password, default is empty, if same with requirepass then add user are admin and no limit from userblacklist
userpass : userpass
 
# command blacklist, limited for login as 'userpass' which cannot use the commands in high risk blacklist
userblacklist : FLUSHALL, SHUTDOWN, KEYS, CONFIG

# [classic | sharding], in classic mode, multiple db is supported
instance-mode : classic

# db count in classic mode, same as Redis
databases : 1

# slot count in each table if use sharding mode
default-slot-num：16

# count of slave replication, support [0, 1, 2, 3, 4], 0 is disable
replication-num : 0

# count of ACKs before response to client, support [0, ...replicaiton-num]，0 is disable
consensus-level : 0

# prefix name of dump when use bgsave command
dump-prefix : backup-
 
# daemonize [yes | no]
daemonize : yes
 
# slotmigrate  [yes | no], not support in pika3.0.0
#slotmigrate : no

# dump directory
dump-path : /data1/pika9001/dump/

# dump expired in days, 0 is never expired
dump-expire: 0

# pidfile path
pidfile : /data1/pika9001/pid/9001.pid
 
# max client connections
maxclients : 20000
 
# sst file size in rocksdb, default is 20M
target-file-size-base : 20971520

# binlog(write2file) expire in days, min is 1 day
expire-logs-days : 7
 
# binlog(write2file) file max count, min is 10
expire-logs-nums : 200
 
# count of root connection
root-connection-num : 2
 
# slow log record time threshold
slowlog-log-slower-than : 10000

# slave readonly (yes/no, 1/0)
# slave-read-only : 0

# sync directory
db-sync-path : ./dbsync/

# max bandwidth to use when sync, range is [1M,1024M], outof range will be set to 1024M
db-sync-speed : -1 (1024MB/s)

# network interface
# network-interface : eth1

# sync config like Redis
# slaveof : master-ip:master-port

# dual master config, can ignore if not use
server-id : 1

# dual master config, can ignore if not use
double-master-ip :	peer ip
double-master-port :	peer port
double-master-server-id :	peer server id
 
# automatic full compact in one day, parameters is $start_hour-$end_hour/disk_free_ratio
# for example: 03-04/30, compact job will start run at 3:00am and stop at 4:00am if disk free more than 30% 
# default empty, no job run
compact-cron : 

# automatic full compact in one day, parameters is $interval_hour/disk_free_ratio
# for example: 4/30, compact job will run every 4 hours if disk free more than 30% 
# default empty, no job run
compact-interval :

# slave priority used by sentinel, default 0
slave-priority : 

# just for binlog compatibility can be [new | old]
# config as new just support as slave of Pika 3.0.0 or above
# config as old can be slave of Pika2.3.3~2.3.5
# default new
identify-binlog-type : new

# traffic control window size, default 9000, max 90000
sync-window-size : 9000

# max buffer size to handle request, can be 67108864(64MB) or 268435456(256MB) or 536870912(512MB)
# default 268435456(256MB)
max-conn-rbuf-size : 268435456

###################
#Critical Settings#
#    Critical     #
###################
# write2file dize, default 100MB, can not modify after start,  limited in [1K, 2G]
binlog-file-size : 104857600

# zip[snappy, zlib, lz4, zstd], default snappy, cannot modify after start
compression : snappy

# flush threads run in background, default 1, range [1, 4]
max-background-flushes : 1

# compact thread count run in background,  default 1, range [1, 4]
max-background-compactions : 1

# max open files which DB can use, default 5000
max-cache-files : 5000

# max size of memtable in rocksdb
[Rocksdb-Basic-Tuning](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
max-write-buffer-size : 10737418240

# max response size to client
max-client-response-size : 1073741824

# storage engine level multiplier, default is 10 times
max-bytes-for-level-multiplier : 10

# hot key statistic count for compaction, 0 is close
max-cache-statistic-keys : 0

# count more than threshold will trigger compaction
small-compaction-threshold : 5000


```
# Data Store Directory

#### db path
Store all data, include kv,set,zset,hash,list. From pika3.0.0 change to hashes，lists，sets，strings，zsets
#### log path 
Store all logs, include info log, error log, warnning log and binlog
#### dump path
Store the snapshot
#### pid path
Store pid file
#### dbsync目录
Store full sync data files