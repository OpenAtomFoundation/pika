package test

var V335MasterInfo = `# Server
pika_version:3.3.5
pika_git_sha:16283243d4d8e87668549d632d6cd13c8ac94016
pika_build_compile_date: Jul  7 2020
os:Linux 3.10.0-862.el7.x86_64 x86_64
arch_bits:64
process_id:312168
tcp_port:9223
thread_num:24
sync_thread_num:6
uptime_in_seconds:83481
uptime_in_days:2
config_file:/redis/pika-v3.3.5/multi/9223/conf/9223.conf
server_id:1

# Data
db_size:1558037767
db_size_human:1485M
log_size:10393611442
log_size_human:9912M
compression:snappy
used_memory:634104203
used_memory_human:604M
db_memtable_usage:469733952
db_tablereader_usage:164370251
db_fatal:0
db_fatal_msg:NULL

# Clients
connected_clients:5

# Stats
total_connections_received:2131
instantaneous_ops_per_sec:3233
total_commands_processed:295232194
is_bgsaving:No
is_scaning_keyspace:No
is_compact:No
compact_cron:
compact_interval:

# Command_Exec_Count
RPUSH:5111302
MSET:5011300
CLIENT:1
LPUSH:10176174
SUBSCRIBE:794
LPOP:5111300
INFO:24980
SPOP:5111300
ZADD:2
SELECT:4
SLAVEOF:2
PING:10462381
AUTH:2103
HSET:5111300
GET:105131100
SET:106399878
MONITOR:4
CONFIG:3
LRANGE:20044400
BGSAVE:1
HMSET:2
SADD:5111300
INCR:5111300
RPOP:5111300
PUBLISH:122339

# CPU
used_cpu_sys:40341.43
used_cpu_user:23306.89
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(MASTER)
role:master
connected_slaves:2
slave0:ip=10.200.14.148,port=9222,conn_fd=131,lag=(db0:0)
slave1:ip=10.200.14.148,port=9221,conn_fd=135,lag=(db0:0)
db0 binlog_offset=143 9122210,safety_purge=write2file133

# Keyspace
# Time:2020-07-23 14:26:33
# Duration: 15s
db0 Strings_keys=83922112, expires=0, invalid_keys=0
db0 Hashes_keys=4887344, expires=0, invalid_keys=0
db0 Lists_keys=1, expires=0, invalid_keys=0
db0 Zsets_keys=1, expires=0, invalid_keys=0
db0 Sets_keys=0, expires=0, invalid_keys=1`