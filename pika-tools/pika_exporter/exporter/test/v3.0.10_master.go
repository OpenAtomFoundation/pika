package test

var V3010MasterInfo = `# Server
pika_version:3.0.10
pika_git_sha:
pika_build_compile_date: Jun 28 2019
os:Linux 3.10.0-957.21.3.el7.x86_64 x86_64
arch_bits:64
process_id:208979
tcp_port:9860
thread_num:10
sync_thread_num:6
uptime_in_seconds:31263736
uptime_in_days:363
config_file:./pika.conf
server_id:1

# Data
db_size:802636507
db_size_human:765M
compression:snappy
used_memory:92202184
used_memory_human:87M
db_memtable_usage:84694544
db_tablereader_usage:7507640

# Log
log_size:1010013663
log_size_human:963M
safety_purge:write2file38101
expire_logs_days:7
expire_logs_nums:10
binlog_offset:38111 55808478

# Clients
connected_clients:8

# Stats
total_connections_received:11141320
instantaneous_ops_per_sec:251
total_commands_processed:16071221446
is_bgsaving:No, , 0
is_scaning_keyspace:Yes, 2020-07-06 14:45:31,0
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
used_cpu_sys:2736371.00
used_cpu_user:3182627.50
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(MASTER)
role:master
connected_slaves:1
slave0:ip=192.168.1.3,port=9860,state=online,sid=1,lag=0
slave1:ip=192.168.1.4,port=9860,state=online,sid=2,lag=0

# Keyspace
# Time: 2020-07-06 14:45:31
# Duration: In Processing
Strings: keys=1178534, expires=1178534, invaild_keys=17232
Hashes: keys=0, expires=0, invaild_keys=0
Lists: keys=0, expires=0, invaild_keys=0
Zsets: keys=0, expires=0, invaild_keys=0
Sets: keys=0, expires=0, invaild_keys=0`