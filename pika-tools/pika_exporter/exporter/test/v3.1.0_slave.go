package test

var V310SlaveInfo = `# Server
pika_version:3.1.0
pika_git_sha:3b5176452779cf6fc7c4891fd247ca8bf3dff515
pika_build_compile_date: May 16 2019
os:Linux 3.10.0-514.el7.x86_64 x86_64
arch_bits:64
process_id:17450
tcp_port:9223
thread_num:1
sync_thread_num:6
uptime_in_seconds:87
uptime_in_days:1
config_file:/home/shunwang/pika/pika-3.1.0/conf/pika.conf
server_id:1

# Data
db_size:2140478
db_size_human:2M
compression:snappy
used_memory:64000
used_memory_human:0M
db_memtable_usage:64000
db_tablereader_usage:0

# Log
log_size:532191
log_size_human:0M
expire_logs_days:7
expire_logs_nums:10
db0:binlog_offset=0 0,safety_purge=none
db1:binlog_offset=0 0,safety_purge=none
db2:binlog_offset=0 0,safety_purge=none
db3:binlog_offset=0 0,safety_purge=none
db4:binlog_offset=0 0,safety_purge=none
db5:binlog_offset=0 0,safety_purge=none
db6:binlog_offset=0 0,safety_purge=none
db7:binlog_offset=0 0,safety_purge=none

# Clients
connected_clients:1

# Stats
total_connections_received:1
instantaneous_ops_per_sec:0
total_commands_processed:1
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
used_cpu_sys:0.28
used_cpu_user:0.55
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(SLAVE)
role:slave
master_host:192.168.107.247
master_port:9223
master_link_status:up
slave_priority:100
slave_read_only:1
repl_state: establish success

# Keyspace
# Time:1970-01-01 08:00:00
db0_Strings: keys=0, expires=0, invaild_keys=0
db0_Hashes: keys=0, expires=0, invaild_keys=0
db0_Lists: keys=0, expires=0, invaild_keys=0
db0_Zsets: keys=0, expires=0, invaild_keys=0
db0_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db1_Strings: keys=0, expires=0, invaild_keys=0
db1_Hashes: keys=0, expires=0, invaild_keys=0
db1_Lists: keys=0, expires=0, invaild_keys=0
db1_Zsets: keys=0, expires=0, invaild_keys=0
db1_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db2_Strings: keys=0, expires=0, invaild_keys=0
db2_Hashes: keys=0, expires=0, invaild_keys=0
db2_Lists: keys=0, expires=0, invaild_keys=0
db2_Zsets: keys=0, expires=0, invaild_keys=0
db2_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db3_Strings: keys=0, expires=0, invaild_keys=0
db3_Hashes: keys=0, expires=0, invaild_keys=0
db3_Lists: keys=0, expires=0, invaild_keys=0
db3_Zsets: keys=0, expires=0, invaild_keys=0
db3_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db4_Strings: keys=0, expires=0, invaild_keys=0
db4_Hashes: keys=0, expires=0, invaild_keys=0
db4_Lists: keys=0, expires=0, invaild_keys=0
db4_Zsets: keys=0, expires=0, invaild_keys=0
db4_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db5_Strings: keys=0, expires=0, invaild_keys=0
db5_Hashes: keys=0, expires=0, invaild_keys=0
db5_Lists: keys=0, expires=0, invaild_keys=0
db5_Zsets: keys=0, expires=0, invaild_keys=0
db5_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db6_Strings: keys=0, expires=0, invaild_keys=0
db6_Hashes: keys=0, expires=0, invaild_keys=0
db6_Lists: keys=0, expires=0, invaild_keys=0
db6_Zsets: keys=0, expires=0, invaild_keys=0
db6_Sets: keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db7_Strings: keys=0, expires=0, invaild_keys=0
db7_Hashes: keys=0, expires=0, invaild_keys=0
db7_Lists: keys=0, expires=0, invaild_keys=0
db7_Zsets: keys=0, expires=0, invaild_keys=0
db7_Sets: keys=0, expires=0, invaild_keys=0`