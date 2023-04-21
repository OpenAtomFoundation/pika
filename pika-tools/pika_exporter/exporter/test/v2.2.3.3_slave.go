package test

var V2233SlaveInfo = `# Server
pika_version:2.2.3.3
os:Linux 3.10.0-957.21.3.el7.x86_64 x86_64
arch_bits:64
process_id:3320
tcp_port:8888
thread_num:6
sync_thread_num:6
uptime_in_seconds:4827986
uptime_in_days:57
config_file:/data/apps/config/pika/pika.conf

# Data
db_size:61609726
db_size_human:58M
compression:snappy
used_memory:51530296
used_memory_human:49M
db_memtable_usage:51269520
db_tablereader_usage:260776

# Log
log_size:1358284883
log_size_human:1295M
safety_purge:write2file1834
expire_logs_days:7
expire_logs_nums:20
binlog_offset:1844 79595083

# Clients
connected_clients:1

# Stats
total_connections_received:1697368
instantaneous_ops_per_sec:3
total_commands_processed:1064238831
is_bgsaving:No, , 0
is_slots_reloading:No, , 0
is_slots_cleanuping:No, , 0
is_scaning_keyspace:No
is_compact:No
compact_cron:04-05/50

# Replication(SLAVE)
role:slave
master_host:192.168.1.2
master_port:8888
master_link_status:up
slave_read_only:1
repl_state: 3

# Keyspace
# Time:1970-01-01 08:00:00
kv keys:0
hash keys:0
list keys:0
zset keys:0
set keys:0`
