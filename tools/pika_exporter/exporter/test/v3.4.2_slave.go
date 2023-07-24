package test

var V342SlaveInfo = `# Server
pika_version:3.4.2
pika_git_sha:bd30511bf82038c2c6531b3d84872c9825fe836a
pika_build_compile_date: Sep  8 2021
os:Linux 3.10.0-693.el7.x86_64 x86_64
arch_bits:64
process_id:138825
tcp_port:9221
thread_num:4
sync_thread_num:6
uptime_in_seconds:8056021
uptime_in_days:94
config_file:/app/pika/pika.conf
server_id:1

# Data
db_size:41977897912
db_size_human:40033M
log_size:5150607654
log_size_human:4912M
compression:snappy
used_memory:1421201619
used_memory_human:1355M
db_memtable_usage:17944432
db_tablereader_usage:1403257187
db_fatal:0
db_fatal_msg:NULL

# Clients
connected_clients:1

# Stats
total_connections_received:331470
instantaneous_ops_per_sec:0
total_commands_processed:7781820120
is_bgsaving:No
is_scaning_keyspace:No
is_compact:No
compact_cron:03-04/30
compact_interval:

# Command_Exec_Count
EXPIREAT:3717952643
INFO:464149
DEL:27429572
SLAVEOF:1
PING:132598
SET:4035576044
CONFIG:132514
COMPACT:1
SLOWLOG:132598

# CPU
used_cpu_sys:120451.99
used_cpu_user:644919.06
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(SLAVE)
role:slave
master_host:192.168.201.81
master_port:9221
master_link_status:up
slave_priority:100
slave_read_only:1
db0 binlog_offset=17794 8127680,safety_purge=write2file17784

# Keyspace
# Time:2023-04-14 01:16:01
# Duration: 37s
db0 Strings_keys=40523556, expires=33332458, invalid_keys=0
db0 Hashes_keys=0, expires=0, invalid_keys=0
db0 Lists_keys=0, expires=0, invalid_keys=0
db0 Zsets_keys=0, expires=0, invalid_keys=0
db0 Sets_keys=0, expires=0, invalid_keys=0`
