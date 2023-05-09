package test

var V335SlaveInfo = `# Server
pika_version:3.3.5
pika_git_sha:16283243d4d8e87668549d632d6cd13c8ac94016
pika_build_compile_date: Jul  7 2020
os:Linux 3.10.0-862.el7.x86_64 x86_64
arch_bits:64
process_id:315022
tcp_port:9221
thread_num:24
sync_thread_num:6
uptime_in_seconds:1359
uptime_in_days:1
config_file:/redis/pika-v3.3.5/multi/9221/conf/9221.conf
server_id:1

# Data
db_size:8884577
db_size_human:8M
log_size:87471185
log_size_human:83M
compression:snappy
used_memory:1464237
used_memory_human:1M
db_memtable_usage:8000
db_tablereader_usage:1456237
db_fatal:0
db_fatal_msg:NULL

# Clients
connected_clients:5

# Stats
total_connections_received:421
instantaneous_ops_per_sec:4
total_commands_processed:7172
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
used_cpu_sys:4.40
used_cpu_user:4.85
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(SLAVE)
role:slave
master_host:10.200.14.148
master_port:9223
master_link_status:up
slave_priority:100
slave_read_only:1
db0 binlog_offset=0 87377376,safety_purge=none

# Keyspace
# Time:1970-01-01 08:00:00
db0 Strings_keys=1000, expires=0, invalid_keys=0
db0 Hashes_keys=0, expires=0, invalid_keys=0
db0 Lists_keys=0, expires=0, invalid_keys=0
db0 Zsets_keys=0, expires=100, invalid_keys=0
db0 Sets_keys=0, expires=0, invalid_keys=0`