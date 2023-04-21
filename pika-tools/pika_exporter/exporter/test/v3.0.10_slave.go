package test

var V3010SlaveInfo = `# Server
pika_version:3.0.10
pika_git_sha:
pika_build_compile_date: Jun 28 2019
os:Linux 3.10.0-957.21.2.el7.x86_64 x86_64
arch_bits:64
process_id:184006
tcp_port:9860
thread_num:10
sync_thread_num:6
uptime_in_seconds:31264509
uptime_in_days:363
config_file:./pika.conf
server_id:1

# Data
db_size:812855003
db_size_human:775M
compression:snappy
used_memory:162700263
used_memory_human:155M
db_memtable_usage:156278608
db_tablereader_usage:6421655

# Log
log_size:1007716371
log_size_human:961M
safety_purge:write2file38101
expire_logs_days:7
expire_logs_nums:10
binlog_offset:38111 55808478

# Clients
connected_clients:7

# Stats
total_connections_received:6805889
instantaneous_ops_per_sec:142
total_commands_processed:16161044193
is_bgsaving:No, , 0
is_scaning_keyspace:Yes, 2020-07-06 14:58:52,0
is_compact:No
compact_cron:04-06/60
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
used_cpu_sys:1387198.00
used_cpu_user:2468313.75
used_cpu_sys_children:0.04
used_cpu_user_children:0.02

# Replication(SLAVE)
role:slave
master_host:192.168.1.2
master_port:9221
master_link_status:up
slave_priority:100
slave_read_only:1
repl_state: connected

# Keyspace
# Time: 2020-07-06 14:58:52
# Duration: In Processing
Strings: keys=1178534, expires=1178534, invaild_keys=21038
Hashes: keys=0, expires=0, invaild_keys=0
Lists: keys=0, expires=0, invaild_keys=0
Zsets: keys=0, expires=0, invaild_keys=0
Sets: keys=0, expires=0, invaild_keys=0`
