package test

var V226SlaveInfo = `# Server
pika_version:2.2.6
pika_git_sha:e646201f53c5584294bdb2dece7b073f0d0e69b2
pika_build_compile_date: Oct 11 2017
os:Linux 3.10.0-514.el7.x86_64 x86_64
arch_bits:64
process_id:16139
tcp_port:9991
thread_num:1
sync_thread_num:6
uptime_in_seconds:2064
uptime_in_days:1
config_file:./pika.conf

# Data
db_size:210537
db_size_human:0M
compression:snappy
used_memory:4216
used_memory_human:0M
db_memtable_usage:4120
db_tablereader_usage:96

# Log
log_size:71951
log_size_human:0M
safety_purge:none
expire_logs_days:7
expire_logs_nums:10
binlog_offset:0 0

# Clients
connected_clients:1

# Stats
total_connections_received:4
instantaneous_ops_per_sec:0
total_commands_processed:6
is_bgsaving:No, , 0
is_slots_reloading:No, , 0
is_scaning_keyspace:No
is_compact:No
compact_cron:
compact_interval:

# Replication(SLAVE)
role:slave
master_host:192.168.1.2
master_port:9921
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