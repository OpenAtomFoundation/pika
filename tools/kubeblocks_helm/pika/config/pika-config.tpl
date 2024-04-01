###########################
# Pika configuration file #
###########################

# Pika port, the default value is 9221.
# [NOTICE] Port Magic offsets of port+1000 / port+2000 are used by Pika at present.
# Port 10221 is used for Rsync, and port 11221 is used for Replication, while the listening port is 9221.
port : 9221

db-instance-num : 3
rocksdb-ttl-second : 86400 * 7;
rocksdb-periodic-second : 86400 * 3;

# Random value identifying the Pika server, its string length must be 40.
# If not set, Pika will generate a random string with a length of 40 random characters.
# run-id :

# Master's run-id
# master-run-id :

# The number of threads for running Pika.
# It's not recommended to set this value exceeds
# the number of CPU cores on the deployment server.
thread-num : 1

# Size of the thread pool, The threads within this pool
# are dedicated to handling user requests.
thread-pool-size : 12

# Size of the low level thread pool, The threads within this pool
# are dedicated to handling slow user requests.
slow-cmd-thread-pool-size : 4

# Slow cmd list e.g. hgetall, mset
slow-cmd-list :

# The number of sync-thread for data replication from master, those are the threads work on slave nodes
# and are used to execute commands sent from master node when replicating.
sync-thread-num : 6

# Directory to store log files of Pika, which contains multiple types of logs,
# Including: INFO, WARNING, ERROR log, as well as binglog(write2fine) file which
# is used for replication.
log-path : ./log/

# Directory to store the data of Pika.
db-path : ./db/

# The size of a single RocksDB memtable at the Pika's bottom layer(Pika use RocksDB to store persist data).
# [Tip] Big write-buffer-size can improve writing performance,
# but this will generate heavier IO load when flushing from buffer to disk,
# you should configure it based on you usage scenario.
# Supported Units [K|M|G], write-buffer-size default unit is in [bytes].
write-buffer-size : 256M

# The size of one block in arena memory allocation.
# If <= 0, a proper value is automatically calculated.
# (usually 1/8 of writer-buffer-size, rounded up to a multiple of 4KB)
# Supported Units [K|M|G], arena-block-size default unit is in [bytes].
arena-block-size :

# Timeout of Pika's connection, counting down starts When there are no requests
# on a connection (it enters sleep state), when the countdown reaches 0, the connection
# will be closed by Pika.
# [Tip] The issue of running out of Pika's connections may be avoided if this value
# is configured properly.
# The Unit of timeout is in [seconds] and its default value is 60(s).
timeout : 60

# The [password of administrator], which is empty by default.
# [NOTICE] If this admin password is the same as user password (including both being empty),
# the value of userpass will be ignored and all users are considered as administrators,
# in this scenario, users are not subject to the restrictions imposed by the userblacklist.
# PS: "user password" refers to value of the parameter below: userpass.
requirepass :

# Password for replication verify, used for authentication when a slave
# connects to a master to request replication.
# [NOTICE] The value of this parameter must match the "requirepass" setting on the master.
masterauth :

# The [password of user], which is empty by default.
# [NOTICE] If this user password is the same as admin password (including both being empty),
# the value of this parameter will be ignored and all users are considered as administrators,
# in this scenario, users are not subject to the restrictions imposed by the userblacklist.
# PS: "admin password" refers to value of the parameter above: requirepass.
# userpass :

# The blacklist of commands for users that logged in by userpass,
# the commands that added to this list will not be available for users except for administrator.
# [Advice] It's recommended to add high-risk commands to this list.
# [Format] Commands should be separated by ",". For example: FLUSHALL, SHUTDOWN, KEYS, CONFIG
# By default, this list is empty.
# userblacklist :

# Running Mode of Pika, The current version only supports running in "classic mode".
# If set to 'classic', Pika will create multiple DBs whose number is the value of configure item "databases".
instance-mode : classic

# The number of databases when Pika runs in classic mode.
# The default database id is DB 0. You can select a different one on
# a per-connection by using SELECT. The db id range is [0, 'databases' value -1].
# The value range of this parameter is [1, 8].
databases : 1

# The number of followers of a master. Only [0, 1, 2, 3, 4] is valid at present.
# By default, this num is set to 0, which means this feature is [not enabled]
# and the Pika runs in standalone mode.
replication-num : 0

# consensus level defines the num of confirms(ACKs) the leader node needs to receive from
# follower nodes before returning the result to the client that sent the request.
# The [value range] of this parameter is: [0, ...replicaiton-num].
# The default value of consensus-level is 0, which means this feature is not enabled.
consensus-level : 0

# The Prefix of dump file's name.
# All the files that generated by command "bgsave" will be name with this prefix.
dump-prefix :

# daemonize  [yes | no].
#daemonize : yes

# The directory to stored dump files that generated by command "bgsave".
dump-path : ./dump/

# TTL of dump files that generated by command "bgsave".
# Any dump files which exceed this TTL will be deleted.
# Unit of dump-expire is in [days] and the default value is 0(day),
# which means dump files never expire.
dump-expire : 0

# Pid file Path of Pika.
pidfile : ./pika.pid

# The Maximum number of Pika's Connection.
maxclients : 20000

# The size of sst file in RocksDB(Pika is based on RocksDB).
# sst files are hierarchical, the smaller the sst file size, the higher the performance and the lower the merge cost,
# the price is that the number of sst files could be huge. On the contrary, the bigger the sst file size, the lower
# the performance and the higher the merge cost, while the number of files is fewer.
# Supported Units [K|M|G], target-file-size-base default unit is in [bytes] and the default value is 20M.
target-file-size-base : 20M

# Expire-time of binlog(write2file) files that stored within log-path.
# Any binlog(write2file) files that exceed this expire time will be cleaned up.
# The unit of expire-logs-days is in [days] and the default value is 7(days).
# The [Minimum value] of this parameter is 1(day).
expire-logs-days : 7

# The maximum number of binlog(write2file) files.
# Once the total number of binlog files exceed this value,
# automatic cleaning will start to ensure the maximum number
# of binlog files is equal to expire-logs-nums.
# The [Minimum value] of this parameter is 10.
expire-logs-nums : 10

# The number of guaranteed connections for root user.
# This parameter guarantees that there are 2(By default) connections available
# for root user to log in Pika from 127.0.0.1, even if the maximum connection limit is reached.
# PS: The maximum connection refers to the parameter above: maxclients.
# The default value of root-connection-num is 2.
root-connection-num : 2

# Slowlog-write-errorlog
slowlog-write-errorlog : no

# The time threshold for slow log recording.
# Any command whose execution time exceeds this threshold will be recorded in pika-ERROR.log,
# which is stored in log-path.
# The unit of slowlog-log-slower-than is in [microseconds(μs)] and the default value is 10000 μs / 10 ms.
slowlog-log-slower-than : 10000

# Slowlog-max-len
slowlog-max-len : 128

# Pika db sync path
db-sync-path : ./dbsync/

# The maximum Transmission speed during full synchronization.
# The exhaustion of network can be prevented by setting this parameter properly.
# The value range of this parameter is [1,1024] with unit in [MB/s].
# [NOTICE] If this parameter is set to an invalid value(smaller than 0 or bigger than 1024),
# it will be automatically reset to 1024.
# The default value of db-sync-speed is -1 (1024MB/s).
db-sync-speed : -1

# The priority of slave node when electing new master node.
# The slave node with [lower] value of slave-priority will have [higher priority] to be elected as the new master node.
# This parameter is only used in conjunction with sentinel and serves no other purpose.
# The default value of slave-priority is 100.
slave-priority : 100

# Specify network interface that work with Pika.
#network-interface : eth1

# The IP and port of the master node are specified by this parameter for
# replication between master and slaves.
# [Format] is "ip:port" , for example: "192.168.1.2:6666" indicates that
# the slave instances that configured with this value will automatically send
# SLAVEOF command to port 6666 of 192.168.1.2 after startup.
# This parameter should be configured on slave nodes.
#slaveof : master-ip:master-port


# Daily/Weekly Automatic full compaction task is configured by compact-cron.
#
#  [Format-daily]: start time(hour)-end time(hour)/disk-free-space-ratio,
#  example: with value of "02-04/60", Pika will perform full compaction task between 2:00-4:00 AM everyday if
#  the disk-free-size / disk-size > 60%.
#
#  [Format-weekly]: week/start time(hour)-end time(hour)/disk-free-space-ratio,
#  example: with value of "3/02-04/60", Pika will perform full compaction task between 2:00-4:00 AM every Wednesday if
#  the disk-free-size / disk-size > 60%.
#
#  [Tip] Automatic full compaction is suitable for scenarios with multiple data structures
#  and lots of items are expired or deleted, or key names are frequently reused.
#
#  [NOTICE]: If compact-interval is set, compact-cron will be masked and disabled.
#
#compact-cron : 3/02-04/60


# Automatic full synchronization task between a time interval is configured by compact-interval.
# [Format]: time interval(hour)/disk-free-space-ratio, example: "6/60", Pika will perform full compaction every 6 hours,
# if the disk-free-size / disk-size > 60%.
# [NOTICE]: compact-interval is prior than compact-cron.
#compact-interval :

# The disable_auto_compactions option is [true | false]
disable_auto_compactions : false

# Rocksdb max_subcompactions
max-subcompactions : 1
# The minimum disk usage ratio for checking resume.
# If the disk usage ratio is lower than min-check-resume-ratio, it will not check resume, only higher will check resume.
# Its default value is 0.7.
#min-check-resume-ratio : 0.7

# The minimum free disk space to trigger db resume.
# If the db has a background error, only the free disk size is larger than this configuration can trigger manually resume db.
# Its default value is 256MB.
# [NOTICE]: least-free-disk-resume-size should not smaller than write-buffer-size!
#least-free-disk-resume-size : 256M

# Manually trying to resume db interval is configured by manually-resume-interval.
# If db has a background error, it will try to manually call resume() to resume db if satisfy the least free disk to resume.
# Its default value is 60 seconds.
#manually-resume-interval : 60

# This window-size determines the amount of data that can be transmitted in a single synchronization process.
# [Tip] In the scenario of high network latency. Increasing this size can improve synchronization efficiency.
# Its default value is 9000. the [maximum] value is 90000.
sync-window-size : 9000

# Maximum buffer size of a client connection.
# [NOTICE] Master and slaves must have exactly the same value for the max-conn-rbuf-size.
# Supported Units [K|M|G]. Its default unit is in [bytes] and its default value is 268435456(256MB). The value range is [64MB, 1GB].
max-conn-rbuf-size : 268435456


#######################################################################E#######
#! Critical Settings !#
#######################################################################E#######

# write_binlog  [yes | no]
write-binlog : yes

# The size of binlog file, which can not be modified once Pika instance started.
# [NOTICE] Master and slaves must have exactly the same value for the binlog-file-size.
# The [value range] of binlog-file-size is [1K, 2G].
# Supported Units [K|M|G], binlog-file-size default unit is in [bytes] and the default value is 100M.
binlog-file-size : 104857600

# Automatically triggers a small compaction according to statistics
# Use the cache to store up to 'max-cache-statistic-keys' keys
# If 'max-cache-statistic-keys' set to '0', that means turn off the statistics function
# and this automatic small compaction feature is disabled.
max-cache-statistic-keys : 0

# When 'delete' or 'overwrite' a specific multi-data structure key 'small-compaction-threshold' times,
# a small compact is triggered automatically if the small compaction feature is enabled.
# small-compaction-threshold default value is 5000 and the value range is [1, 100000].
small-compaction-threshold : 5000
small-compaction-duration-threshold : 10000

# The maximum total size of all live memtables of the RocksDB instance that owned by Pika.
# Flushing from memtable to disk will be triggered if the actual memory usage of RocksDB
# exceeds max-write-buffer-size when next write operation is issued.
# [RocksDB-Basic-Tuning](https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning)
# Supported Units [K|M|G], max-write-buffer-size default unit is in [bytes].
max-write-buffer-size : 10737418240

# The maximum number of write buffers(memtables) that are built up in memory for one ColumnFamily in DB.
# The default and the minimum number is 2. It means that Pika(RocksDB) will write to a write buffer
# when it flushes the data of another write buffer to storage.
# If max-write-buffer-num > 3, writing will be slowed down.
max-write-buffer-num : 2

# `min_write_buffer_number_to_merge` is the minimum number of memtables
# that need to be merged before placing the order. For example, if the
# option is set to 2, immutable memtables will only be flushed if there
# are two of them - a single immutable memtable will never be flushed.
# If multiple memtables are merged together, less data will be written
# to storage because the two updates are merged into a single key. However,
# each Get() must linearly traverse all unmodifiable memtables and check
# whether the key exists. Setting this value too high may hurt performance.
min-write-buffer-number-to-merge : 1

# rocksdb level0_stop_writes_trigger
level0-stop-writes-trigger : 36

# rocksdb level0_slowdown_writes_trigger
level0-slowdown-writes-trigger : 20

# rocksdb level0_file_num_compaction_trigger
level0-file-num-compaction-trigger : 4

# The maximum size of the response package to client to prevent memory
# exhaustion caused by commands like 'keys *' and 'Scan' which can generate huge response.
# Supported Units [K|M|G]. The default unit is in [bytes].
max-client-response-size : 1073741824

# The compression algorithm. You can not change it when Pika started.
# Supported types: [snappy, zlib, lz4, zstd]. If you do not wanna compress the SST file, please set its value as none.
# [NOTICE] The Pika official binary release just linking the snappy library statically, which means that
# you should compile the Pika from the source code and then link it with other compression algorithm library statically by yourself.
compression : snappy

# if the vector size is smaller than the level number, the undefined lower level uses the
# last option in the configurable array, for example, for 3 level
# LSM tree the following settings are the same:
# configurable array: [none:snappy]
# LSM settings: [none:snappy:snappy]
# When this configurable is enabled, compression is ignored,
# default l0 l1 noCompression, l2 and more use `compression` option
# https://github.com/facebook/rocksdb/wiki/Compression
#compression_per_level : [none:none:snappy:lz4:lz4]

# The number of background flushing threads.
# max-background-flushes default value is 1 and the value range is [1, 4].
max-background-flushes : 1

# The number of background compacting threads.
# max-background-compactions default value is 2 and the value range is [1, 8].
max-background-compactions : 2

# The number of background threads.
# max-background-jobs default value is 3 and the value range is [2, 12].
max-background-jobs : 3

# maximum value of RocksDB cached open file descriptors
max-cache-files : 5000

# The ratio between the total size of RocksDB level-(L+1) files and the total size of RocksDB level-L files for all L.
# Its default value is 10(x). You can also change it to 5(x).
max-bytes-for-level-multiplier : 10

# slotmigrate is mainly used to migrate slots, usually we will set it to no.
# When you migrate slots, you need to set it to yes, and reload slotskeys before.
# slotmigrate  [yes | no]
slotmigrate : no

# slotmigrate thread num
slotmigrate-thread-num : 8

# thread-migrate-keys-num  1/8 of the write_buffer_size_
thread-migrate-keys-num : 64

# BlockBasedTable block_size, default 4k
# block-size: 4096

# block LRU cache, default 8M, 0 to disable
# Supported Units [K|M|G], default unit [bytes]
# block-cache: 8388608

# num-shard-bits default -1, the number of bits from cache keys to be use as shard id.
# The cache will be sharded into 2^num_shard_bits shards.
# https://github.com/EighteenZi/rocksdb_wiki/blob/master/Block-Cache.md#lru-cache
# num-shard-bits: -1

# whether the block cache is shared among the RocksDB instances, default is per CF
# share-block-cache: no

# The slot number of pika when used with codis.
default-slot-num : 1024

# whether or not index and filter blocks is stored in block cache
# cache-index-and-filter-blocks: no

# pin_l0_filter_and_index_blocks_in_cache [yes | no]
# When `cache-index-and-filter-blocks` is enabled, `pin_l0_filter_and_index_blocks_in_cache` is suggested to be enabled
# pin_l0_filter_and_index_blocks_in_cache : no

# when set to yes, bloomfilter of the last level will not be built
# optimize-filters-for-hits: no
# https://github.com/facebook/rocksdb/wiki/Leveled-Compaction#levels-target-size
# level-compaction-dynamic-level-bytes: no

################################## RocksDB Rate Limiter #######################
# rocksdb rate limiter
# https://rocksdb.org/blog/2017/12/18/17-auto-tuned-rate-limiter.html
# https://github.com/EighteenZi/rocksdb_wiki/blob/master/Rate-Limiter.md
#######################################################################E#######

# rate limiter bandwidth, default 2000MB/s
#rate-limiter-bandwidth : 2097152000

#rate-limiter-refill-period-us : 100000
#
#rate-limiter-fairness: 10

# rate limiter auto tune https://rocksdb.org/blog/2017/12/18/17-auto-tuned-rate-limiter.html. the default value is false.
#rate-limiter-auto-tuned : true

################################## RocksDB Blob Configure #####################
# rocksdb blob configure
# https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
# wiki https://github.com/facebook/rocksdb/wiki/BlobDB
#######################################################################E#######

# enable rocksdb blob, default no
# enable-blob-files : yes

# values at or above this threshold will be written to blob files during flush or compaction.
# Supported Units [K|M|G], default unit is in [bytes].
# min-blob-size : 4K

# the size limit for blob files
# Supported Units [K|M|G], default unit is in [bytes].
# blob-file-size : 256M

# the compression type to use for blob files. All blobs in the same file are compressed using the same algorithm.
# Supported types: [snappy, zlib, lz4, zstd]. If you do not wanna compress the SST file, please set its value as none.
# [NOTICE] The Pika official binary release just link the snappy library statically, which means that
# you should compile the Pika from the source code and then link it with other compression algorithm library statically by yourself.
# blob-compression-type : lz4

# set this to open to make BlobDB actively relocate valid blobs from the oldest blob files as they are encountered during compaction.
# The value option is [yes | no]
# enable-blob-garbage-collection : no

# the cutoff that the GC logic uses to determine which blob files should be considered “old“.
# This parameter can be tuned to adjust the trade-off between write amplification and space amplification.
# blob-garbage-collection-age-cutoff : 0.25

# if the ratio of garbage in the oldest blob files exceeds this threshold,
# targeted compactions are scheduled in order to force garbage collecting the blob files in question
# blob_garbage_collection_force_threshold : 1.0

# the Cache object to use for blobs, default not open
# blob-cache : 0

# blob-num-shard-bits default -1, the number of bits from cache keys to be use as shard id.
# The cache will be sharded into 2^blob-num-shard-bits shards.
# blob-num-shard-bits : -1

# Rsync Rate limiting configuration 200MB/s
throttle-bytes-per-second : 207200000
max-rsync-parallel-num : 4

# The synchronization mode of Pika primary/secondary replication is determined by ReplicationID. ReplicationID in one replication_cluster are the same
# replication-id :

###################
## Cache Settings
###################
# the number of caches for every db
cache-num : 16

# cache-model 0:cache_none 1:cache_read
cache-model : 1
# cache-type: string, set, zset, list, hash, bit
cache-type: string, set, zset, list, hash, bit

# Maximum number of keys in the zset redis cache
# On the disk DB, a zset field may have many fields. In the memory cache, we limit the maximum
# number of keys that can exist in a zset,  which is zset-zset-cache-field-num-per-key, with a
# default value of 512.
zset-cache-field-num-per-key : 512

# If the number of elements in a zset in the DB exceeds zset-cache-field-num-per-key,
# we determine whether to cache the first 512[zset-cache-field-num-per-key] elements
# or the last 512[zset-cache-field-num-per-key] elements in the zset based on zset-cache-start-direction.
#
# If zset-cache-start-direction is 0, cache the first 512[zset-cache-field-num-per-key] elements from the header
# If zset-cache-start-direction is -1, cache the last 512[zset-cache-field-num-per-key] elements
zset-cache-start-direction : 0

# the cache maxmemory of every db, configuration 10G
cache-maxmemory : 10737418240

# cache-maxmemory-policy
# 0: volatile-lru -> Evict using approximated LRU among the keys with an expire set.
# 1: allkeys-lru -> Evict any key using approximated LRU.
# 2: volatile-lfu -> Evict using approximated LFU among the keys with an expire set.
# 3: allkeys-lfu -> Evict any key using approximated LFU.
# 4: volatile-random -> Remove a random key among the ones with an expire set.
# 5: allkeys-random -> Remove a random key, any key.
# 6: volatile-ttl -> Remove the key with the nearest expire time (minor TTL)
# 7: noeviction -> Don't evict anything, just return an error on write operations.
cache-maxmemory-policy : 1

# cache-maxmemory-samples
cache-maxmemory-samples: 5

# cache-lfu-decay-time
cache-lfu-decay-time: 1


# is possible to manage access to Pub/Sub channels with ACL rules as well. The
# default Pub/Sub channels permission if new users is controlled by the
# acl-pubsub-default configuration directive, which accepts one of these values:
#
# allchannels: grants access to all Pub/Sub channels
# resetchannels: revokes access to all Pub/Sub channels
#
# acl-pubsub-default defaults to 'resetchannels' permission.
# acl-pubsub-default : resetchannels

# ACL users are defined in the following format:
# user : <username> ... acl rules ...
#
# For example:
#
# user : worker on >password ~key* +@all

# Using an external ACL file
#
# Instead of configuring users here in this file, it is possible to use
# a stand-alone file just listing users. The two methods cannot be mixed:
# if you configure users here and at the same time you activate the external
# ACL file, the server will refuse to start.
#
# The format of the external ACL user file is exactly the same as the
# format that is used inside pika.conf to describe users.
#
# aclfile : ../conf/users.acl

# (experimental)
# It is possible to change the name of dangerous commands in a shared environment.
# For instance the CONFIG command may be renamed into something Warning: To prevent
# data inconsistency caused by different configuration files, do not use the rename
# command to modify write commands on the primary and secondary servers. If necessary,
# ensure that the configuration files of the primary and secondary servers are consistent
# In addition, when using the command rename, you must not use ""  to modify the command,
# for example, rename-command: FLUSHDB "360flushdb" is incorrect; instead, use
# rename-command: FLUSHDB 360flushdb is correct. After the rename command is executed,
# it is most appropriate to use a numeric string with uppercase or lowercase letters
# for example: rename-command : FLUSHDB joYAPNXRPmcarcR4ZDgC81TbdkSmLAzRPmcarcR
# Warning: Currently only applies to flushdb, slaveof, bgsave, shutdown, config command
# Warning: Ensure that the Settings of rename-command on the master and slave servers are consistent
#
# Example:
# rename-command : FLUSHDB 360flushdb
