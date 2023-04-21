package test

var V236MasterInfo = `# Server
pika_version:2.3.6
pika_git_sha:37cbaa7d77bc05dc3d9dc6981d583ab8c73685ba
pika_build_compile_date: Apr  8 2019
os:Linux 2.6.32-642.el6.x86_64 x86_64
arch_bits:64
process_id:13617
tcp_port:9221
thread_num:20
sync_thread_num:6
uptime_in_seconds:37264509
uptime_in_days:432
config_file:./pika.conf
server_id:1

# Data
db_size:136447767106
db_size_human:130126M
compression:snappy
used_memory:1013140688
used_memory_human:966M
db_memtable_usage:21066064
db_tablereader_usage:992074624

# Log
log_size:174797679785
log_size_human:166700M
safety_purge:write2file321754
expire_logs_days:7
expire_logs_nums:10
binlog_offset:321764 81941349

# Clients
connected_clients:244

# Stats
total_connections_received:86734154
instantaneous_ops_per_sec:1101
total_commands_processed:37625767257
is_bgsaving:No, , 0
is_slots_reloading:No, , 0
is_slots_cleanuping:No, , 0
is_scaning_keyspace:Yes, 2020-06-28 17:03:59,0
is_compact:No
compact_cron:
compact_interval:

# CPU
used_cpu_sys:1478266.88
used_cpu_user:2210061.00
used_cpu_sys_children:0.00
used_cpu_user_children:0.00

# Replication(MASTER)
role:master
connected_slaves:1
slave0:ip=192.168.1.2,port=9221,state=online,sid=1,lag=0
slave1:ip=192.168.1.3,port=9221,state=offline,sid=2,lag=1000

# Keyspace
# Time:2020-06-28 17:03:59
kv keys:234093002
hash keys:0
list keys:0
zset keys:0
set keys:0`