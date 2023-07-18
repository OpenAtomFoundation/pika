package test

var V342MasterInfo = `# Server
pika_version:3.4.2
pika_git_sha:bd30511bf82038c2c6531b3d84872c9825fe836a
pika_build_compile_date: Sep  8 2021
os:Linux 3.10.0-693.el7.x86_64 x86_64
arch_bits:64
process_id:12549
tcp_port:9221
thread_num:4
sync_thread_num:6
uptime_in_seconds:8056286
uptime_in_days:94
config_file:/app/pika/pika-9221.conf
server_id:1

# Data
db_size:41971885221
db_size_human:40027M
log_size:5150573069
log_size_human:4911M
compression:snappy
used_memory:1445394489
used_memory_human:1378M
db_memtable_usage:42493512
db_tablereader_usage:1402900977
db_fatal:0
db_fatal_msg:NULL

# Clients
connected_clients:7

# Stats
total_connections_received:496042
instantaneous_ops_per_sec:106
total_commands_processed:11590807682
is_bgsaving:No
is_scaning_keyspace:No
is_compact:No
compact_cron:03-04/30
compact_interval:

# Command_Exec_Count
INFO:464159
DEL:27429572
PING:2033416
EXPIRE:3717952643
GET:3807086732
SET:4035576044
HGETALL:1
CONFIG:132516
SLOWLOG:132599

# CPU
used_cpu_sys:226152.34
used_cpu_user:842762.56
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(MASTER)
role:master
connected_slaves:1
slave0:ip=192.168.201.82,port=9221,conn_fd=88,lag=(db0:0)
db0 binlog_offset=17794 8127680,safety_purge=write2file17784 
consensus last_log=11111111111

# Keyspace
# Time:2023-04-14 01:16:01
# Duration: 41s
db0 Strings_keys=40523556, expires=33332598, invalid_keys=0
db0 Hashes_keys=0, expires=0, invalid_keys=0
db0 Lists_keys=0, expires=0, invalid_keys=0
db0 Zsets_keys=0, expires=0, invalid_keys=0
db0 Sets_keys=0, expires=0, invalid_keys=0`
