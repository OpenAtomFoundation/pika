package test

var V320MasterInfo = `# Server
pika_version:3.2.0
pika_git_sha:7aba4c00dec292e248cc36a1747497b057eb3dbb
pika_build_compile_date: Jul 22 2019
os:Linux 3.10.0-514.el7.x86_64 x86_64
arch_bits:64
process_id:11959
tcp_port:9222
thread_num:1
sync_thread_num:6
uptime_in_seconds:476
uptime_in_days:1
config_file:/home/shunwang/pika/pika-3.2.0/conf/pika.conf
server_id:1

# Data
db_size:2484010
db_size_human:2M
log_size:540289
log_size_human:0M
compression:snappy
used_memory:64000
used_memory_human:0M
db_memtable_usage:64000
db_tablereader_usage:0

# Clients
connected_clients:1

# Stats
total_connections_received:11
instantaneous_ops_per_sec:0
total_commands_processed:4
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
used_cpu_user:0.39
used_cpu_sys_children:0.01
used_cpu_user_children:0.00

# Replication(MASTER)
role:master
connected_slaves:1
slave0:ip=192.168.107.248,port=9222,conn_fd=1345,lag=(db0:0)(db1:0)(db2:110)(db3:0)(db4:0)(db5:0)(db6:0)(db7:0)
slave1:ip=192.168.107.249,port=9222,conn_fd=6580,lag=(db0:100)(db1:0)(db2:0)(db3:1000)(db4:0)(db5:0)(db6:0)(db7:0)
db0 binlog_offset=100 0,safety_purge=write2file100
db1 binlog_offset=0 0,safety_purge=none
db2 binlog_offset=0 0,safety_purge=none
db3 binlog_offset=10 1322,safety_purge=write2file133
db4 binlog_offset=0 0,safety_purge=none
db5 binlog_offset=0 0,safety_purge=none
db6 binlog_offset=0 0,safety_purge=none
db7 binlog_offset=0 0,safety_purge=none

# Keyspace
# Time:1970-01-01 08:00:00
db0 Strings_keys=0, expires=0, invaild_keys=0
db0 Hashes_keys=0, expires=0, invaild_keys=0
db0 Lists_keys=0, expires=0, invaild_keys=0
db0 Zsets_keys=0, expires=0, invaild_keys=0
db0 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db1 Strings_keys=0, expires=0, invaild_keys=0
db1 Hashes_keys=0, expires=0, invaild_keys=0
db1 Lists_keys=0, expires=0, invaild_keys=0
db1 Zsets_keys=0, expires=0, invaild_keys=0
db1 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db2 Strings_keys=0, expires=0, invaild_keys=0
db2 Hashes_keys=0, expires=0, invaild_keys=0
db2 Lists_keys=0, expires=0, invaild_keys=0
db2 Zsets_keys=0, expires=0, invaild_keys=0
db2 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db3 Strings_keys=0, expires=0, invaild_keys=0
db3 Hashes_keys=0, expires=0, invaild_keys=0
db3 Lists_keys=0, expires=0, invaild_keys=0
db3 Zsets_keys=0, expires=0, invaild_keys=0
db3 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db4 Strings_keys=0, expires=0, invaild_keys=0
db4 Hashes_keys=0, expires=0, invaild_keys=0
db4 Lists_keys=0, expires=0, invaild_keys=0
db4 Zsets_keys=0, expires=0, invaild_keys=0
db4 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db5 Strings_keys=0, expires=0, invaild_keys=0
db5 Hashes_keys=0, expires=0, invaild_keys=0
db5 Lists_keys=0, expires=0, invaild_keys=0
db5 Zsets_keys=0, expires=0, invaild_keys=0
db5 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db6 Strings_keys=0, expires=0, invaild_keys=0
db6 Hashes_keys=0, expires=0, invaild_keys=0
db6 Lists_keys=0, expires=0, invaild_keys=0
db6 Zsets_keys=0, expires=0, invaild_keys=0
db6 Sets_keys=0, expires=0, invaild_keys=0

# Time:1970-01-01 08:00:00
db7 Strings_keys=0, expires=0, invaild_keys=0
db7 Hashes_keys=0, expires=0, invaild_keys=0
db7 Lists_keys=0, expires=0, invaild_keys=0
db7 Zsets_keys=0, expires=0, invaild_keys=0
db7 Sets_keys=0, expires=0, invaild_keys=0`