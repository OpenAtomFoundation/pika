package test

var V2233MasterInfo = `# Server
pika_version:2.2.3.3
os:Linux 3.10.0-957.21.3.el7.x86_64 x86_64
arch_bits:64
process_id:3652
tcp_port:8888
thread_num:6
sync_thread_num:6
uptime_in_seconds:4827977
uptime_in_days:57
config_file:/data/apps/config/pika/pika.conf

# Data
db_size:76365887
db_size_human:72M
compression:snappy
used_memory:52563416
used_memory_human:50M
db_memtable_usage:51239488
db_tablereader_usage:1323928

# Log
log_size:2170115968
log_size_human:2069M
safety_purge:write2file1834
expire_logs_days:7
expire_logs_nums:20
binlog_offset:1844 79515877

# Clients
connected_clients:513

# Stats
total_connections_received:1802767
instantaneous_ops_per_sec:216
total_commands_processed:2100008430
is_bgsaving:No, , 0
is_slots_reloading:No, , 0
is_slots_cleanuping:No, , 0
is_scaning_keyspace:No
is_compact:No
compact_cron:04-05/50

# Replication(MASTER)
role:master
connected_slaves:1
slave0:ip=192.168.1.1,port=8888,state=online

# Keyspace
# Time:1970-01-01 08:00:00
kv keys:0
hash keys:0
list keys:0
zset keys:0
set keys:0`
