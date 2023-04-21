package test

var V3016SlaveInfo = `# Server
pika_version:3.0.16
pika_git_sha:2c1267e7d1bf2ed7d20d5e16d8828c1920db9e3e
pika_build_compile_date: Dec 20 2019
os:Linux 3.10.0-514.el7.x86_64 x86_64
arch_bits:64
process_id:17787
tcp_port:9224
thread_num:1
sync_thread_num:6
uptime_in_seconds:88
uptime_in_days:1
config_file:/home/shunwang/pika/pika-3.0.16/conf/pika.conf
server_id:1

# Data
db_size:267368
db_size_human:0M
compression:snappy
used_memory:8000
used_memory_human:0M
db_memtable_usage:8000
db_tablereader_usage:0
db_fatal:0
db_fatal_msg:NULL

# Log
log_size:70481
log_size_human:0M
safety_purge:none
expire_logs_days:7
expire_logs_nums:10
binlog_offset:0 0

# Clients
connected_clients:1

# Stats
total_connections_received:1
instantaneous_ops_per_sec:0
total_commands_processed:2
is_bgsaving:No, , 0
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
used_cpu_sys:0.06
used_cpu_user:0.07
used_cpu_sys_children:0.02
used_cpu_user_children:0.01

# Replication(SLAVE)
role:slave
master_host:192.168.107.247
master_port:9224
master_link_status:up
slave_priority:100
slave_read_only:1
repl_state: connected

# Keyspace
# Time: 1970-01-01 08:00:00
Strings: keys=0, expires=0, invaild_keys=0
Hashes: keys=0, expires=0, invaild_keys=0
Lists: keys=0, expires=0, invaild_keys=0
Zsets: keys=0, expires=0, invaild_keys=0
Sets: keys=0, expires=0, invaild_keys=0`