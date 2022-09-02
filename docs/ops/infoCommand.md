Execute `INFO` Command

```
127.0.0.1:9221> info [section]
```

```
# Master：
# Server
pika_version:2.3.0 -------------------------------------------- pika version
pika_git_sha:3668a2807a3d047ea43656b58a2130c1566eeb65 --------- git sha
pika_build_compile_date: Nov 14 2017 -------------------------- compile date
os:Linux 2.6.32-2.0.0.8-6 x86_64 ------------------------------ OS
arch_bits:64 -------------------------------------------------- OS arch
process_id:12969 ---------------------------------------------- pika pid
tcp_port:9001 ------------------------------------------------- pika port
thread_num:12 ------------------------------------------------- pika threads
sync_thread_num:6 --------------------------------------------- sync threads count
uptime_in_seconds:3074 ---------------------------------------- pika run time
uptime_in_days:0 ---------------------------------------------- pika run in day
config_file:/data1/pika9001/pika9001.conf --------------------- pika conf location
server_id:1 --------------------------------------------------- pika的server id

# Data
db_size:770439 ------------------------------------------------ db size (Byte)
db_size_human:0M ---------------------------------------------- db size for human(M)
compression:snappy -------------------------------------------- compression
used_memory:4248 ---------------------------------------------- memory(Byte)
used_memory_human:0M ------------------------------------------ memory in human(M)
db_memtable_usage:4120 ---------------------------------------- memtable usage(Byte)
db_tablereader_usage:128 -------------------------------------- tablereader usage(Byte)

# Log
log_size:110174 ----------------------------------------------- binlog size(Byte)
log_size_human:0M --------------------------------------------- binlog size for human(M)
safety_purge:none --------------------------------------------- safe delete file
expire_logs_days:7 -------------------------------------------- binlog expire in days
expire_logs_nums:10 ------------------------------------------- binlog expire file count
binlog_offset:0 388 ------------------------------------------- binlog offset(file,offset)
 
# Clients
connected_clients:2 ------------------------------------------- connections
 
# Stats
total_connections_received:18 --------------------------------- all connections
instantaneous_ops_per_sec:1 ----------------------------------- QPS
total_commands_processed:633 ---------------------------------- all requests
is_bgsaving:No, , 0 ------------------------------------------- pika backup
is_scaning_keyspace:No ---------------------------------------- is scan
is_compact:No ------------------------------------------------- is compact
compact_cron: ------------------------------------------------- compact(format: start-end/ratio, eg. 02-04/60)
compact_interval: --------------------------------------------- compact interval(format: interval/ratio, eg. 6/60)

# CPU
used_cpu_sys:48.52 -------------------------------------------- sys CPU in Pika
used_cpu_user:73.10 ------------------------------------------- user CPU in Pika
used_cpu_sys_children:0.05 ------------------------------------ children sys in Pika
used_cpu_user_children:0.05 ----------------------------------- children user CPU in Pika
 
# Replication(MASTER)
role:master --------------------------------------------------- role
connected_slaves:1 -------------------------------------------- count of slaves
slave0:ip=192.168.1.1,port=57765,state=online,sid=2,lag=0 ----- lags
 
# Slaves
# Replication(SLAVE)
role:slave ---------------------------------------------------- role
master_host:192.168.1.2 --------------------------------------- master IP
master_port:9001 ---------------------------------------------- master port
master_link_status:up ----------------------------------------- status
slave_read_only:1 --------------------------------------------- is readonly
repl_state: connected ----------------------------------------- repl state
 
# Keyspace
# Time:2016-04-22 17:08:33 ------------------------------------ last statistic
db0 Strings_keys=100004, expires=0, invaild_keys=0
db0 Hashes_keys=2, expires=0, invaild_keys=0
db0 Lists_keys=0, expires=0, invaild_keys=0
db0 Zsets_keys=1, expires=0, invaild_keys=0
db0 Sets_keys=0, expires=0, invaild_keys=0
# keys：same as Redis
# expires：same as Redis
# invalid_keys：only in Pika, tag as deleted but still use space , can try compact to clear

```
