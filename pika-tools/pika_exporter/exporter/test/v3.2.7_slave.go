package test

var V327SlaveInfo = `# Server
pika_version:3.2.7
pika_git_sha:b22b0561f9093057d2e2d5cc783ff630fb2c8884
pika_build_compile_date: Nov  7 2019
os:Linux 3.10.0-1062.9.1.el7.x86_64 x86_64
arch_bits:64
process_id:1699
tcp_port:60501
thread_num:6
sync_thread_num:6
uptime_in_seconds:70603
uptime_in_days:2
config_file:/usr/local/etc/pika.conf
server_id:1

# Data
db_size:5821711686
db_size_human:5552M
log_size:3163801180
log_size_human:3017M
compression:snappy
used_memory:673276158
used_memory_human:642M
db_memtable_usage:615568032
db_tablereader_usage:57708126
db_fatal:0
db_fatal_msg:NULL

# Clients
connected_clients:1

# Stats
total_connections_received:2497
instantaneous_ops_per_sec:1422
total_commands_processed:77588095
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
used_cpu_sys:3659.24
used_cpu_user:12147.08
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(SLAVE)
role:slave
master_host:10.111.219.134
master_port:60500
master_link_status:up
slave_priority:100
slave_read_only:1
db0 binlog_offset=5462 16676965,safety_purge=write2file5452

# Keyspace
# Time:2019-12-09 20:07:05
# Duration: 16s
db0 Strings_keys=1046836, expires=1046836, invaild_keys=10361
db0 Hashes_keys=0, expires=0, invaild_keys=2
db0 Lists_keys=0, expires=0, invaild_keys=1
db0 Zsets_keys=0, expires=0, invaild_keys=1
db0 Sets_keys=0, expires=0, invaild_keys=1`