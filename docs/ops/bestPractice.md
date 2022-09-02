### This best pratice draft is from 360 company and community feedbacks
> can star our project to mark as favourite

**Best Practice 0:**

Please tell us the version if you ask question in Pika group（QQ group:294254078）

**Best Practice 1:**

We suggest you use version more than v3.X or v2.3.6(v2.0 not support now)

**Best Practice 2:**

The thread number should be config as CPU number if you deploy one Pika instance in one server. All thread number should be config as share CPU number if you deploy multiple Pika instance to one server.

**Best Practice 4:**

The performance is very depend on disk IO, so we do not suggest deploy Pika in SATA if you expect low latency, we suggest master/slave have same hardware.

**Best Practice 5:**

If you use hash type in Pika, suggest field count less than 10k.

**Best Practice 6:**

`root-connection-num` is very useful, it is means allow connection count from IP 127.0.0.1. If `maxclients` connections are full, you also can login Pika as root connection from 127.0.0.1.

**Best Practice 7:**

`client kill` has enhenced version, you can kill all connections except self use `client kill all`

**Best Practice 8:**

Reasonable config `timeout` can reduce memory usage because every client connection use some memory.

**Best Practice 9:**

Memory usage in Pika have two part, one part is SST file's page cache, another is connections use. If your Pika instance use very huge memory, it may be leaked, you can use `client kill all` and `tcmalloc free` to force free.

**Best Practice 10:**

Pika singular is not suggested. You can consider master/slave mode.

**Best Practice 11:**

Dual master mode is strongly not suggested.

**Best Practice 12:**

Turn off binlog (config `write-binlog` as no) in singular can have best write performance but not suggested.

**Best Practice 13:**

More larger `open_file_limit` should be config because the count of SST files will grow up. If you prefer to have larger SST file size and less file count you can use `target-file-size-base`.

**Best Practice 14:**

Do not touch `write2file` and `manifest` files, they are all very important files in binlog.

**Best Practice 15:**

Sync binlog in Pika use rsync tool, we can config `db-sync-speed` to limit rate in MB.

**Best Practice 16:**

Command `key *` will use affect many key and use huge memory, take care. 

**Best Practice 17:**

If `info keyspace` print 0, you should execute command `info keyspace 1` firstly, it will run an async job and you can execute `info stats` and see if `is_scaning_keyspace` is `yes` or `no`. The result store in memory and will be clear after restart.

**Best Practice 18:**

Don't execute `info keyspace 1` or `keys *` when run `compact`.

**Best Practice 19:**

The config item `compact-cron` can reduce impact on performance, after v3.0 you can use `auto_compact`
There are unused data risks on performance:
1. Huge data capacity, can try `compact` and see if it is properly cleared.
1. Latency increase sharply, can try `compact` and see if it is recovered.

**Best Practice 20:**

Expired keys will be statistic in v3.0(use `info keyspace 1` to trigger and see result by `info keyspace`), the `invaild_keys` is the count already deleted/expired but not physical deleted , if this count is larger can clear by execute `compact`.

**Best Practice 21:**

`write2file` means `binlog`,should ajust the count and retention properly, we suggest at least 48 hours.

**Best Practice 22:**

If heavy traffic write in master, slave maybe delay when sync data. You can tune `sync-thread-num`, but hotspot keys can not help.  

**Best Practice 23:**

Backup in Pike usually use snapshot which save to dump path and named with date suffix. Pika will block write when dumping, and client will be have higher latency.

**Best Practice 24:**

Heave write may lead to protect mode in rocksdb(memtable cannot flush in time), you should write slowly or use SSD. Tuning `write-buffer-size` is another choice, you can try.

**Best Practice 25:**

Compress in Pika default is `snappy`, you can change to `zlib` also. If you donot use compress can reduce the CPU usage but need more space. You can try compress in client side.

**Best Practice 26:**

R/W split is important in Pika. You can add more slaves to handle more read traffic and reduce latency.

**Best Practice 27:**

When you execute command `compact`, you can find disk usage increase firstly and then decrease, so extra disk space is needed before compact. You also can try compact some data structure, like  `compact set`.

**Best Practice 28:**

Before `compact` you should have 2 times space, you can delete backup files.

**Best Practice 29:**

Like Redis, in Pika you also can use `slowlog` to get slow logs. More space needed if you config high threshold in `slowlog`. In Pika, slowlog can print in error log if you config `slowlog-write-errorlog` as yes.

**Best Practice 30:**

In Pika the `rename-command`）is not allow. 

**Best Practice 31:**

After Pika v3.0.7 network IO thread config as `thread-num`, data write operation config as `thread-pool-size`, you can tunning, if have heavy operation you can use 2 times than IO thread, `thread-pool-size = 2 * thread-num`

**Best Practice 32:**

Since v3.0.5, Pika have more policy to monitor and compact in hash，set，zset，list:
* `max-cache-statistic-keys`        Max monitor keys, 10000 etc.
* `small-compaction-threshold`      Config how many field modified, 500 etc.


**Best Practice 33:**

Do not `compact` in heavy traffic, if run in misoperation, can try restart Pika instance, it is safe.

**Best Practice 34:**

The reason of loop sync from slave node maybe is binlog in master not long enough, can config expire-logs-nums to bigger.