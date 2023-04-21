package test

var V236SlaveInfo = `# Server
pika_version:2.3.6
pika_git_sha:37cbaa7d77bc05dc3d9dc6981d583ab8c73685ba
pika_build_compile_date: Apr  8 2019
os:Linux 2.6.32-642.el6.x86_64 x86_64
arch_bits:64
process_id:19355
tcp_port:9291
thread_num:10
sync_thread_num:6
uptime_in_seconds:22488439
uptime_in_days:261
config_file:./pika.conf
server_id:1

# Data
db_size:214315888
db_size_human:204M
compression:snappy
used_memory:196611664
used_memory_human:187M
db_memtable_usage:196494096
db_tablereader_usage:117568

# Log
log_size:1297591770
log_size_human:1237M
safety_purge:write2file46867
expire_logs_days:7
expire_logs_nums:10
binlog_offset:46877 42980297

# Clients
connected_clients:20

# Stats
total_connections_received:5805379
instantaneous_ops_per_sec:4623
total_commands_processed:33050865828
is_bgsaving:No, , 0
is_slots_reloading:No, , 0
is_slots_cleanuping:No, , 0
is_scaning_keyspace:No
is_compact:No
compact_cron:
compact_interval:

# CPU
used_cpu_sys:530741.00
used_cpu_user:1675350.75
used_cpu_sys_children:0.02
used_cpu_user_children:0.01

# Replication(SLAVE)
role:slave
master_host:192.168.111.109
master_port:9221
master_link_status:up
slave_priority:100
slave_read_only:1
repl_state: 3

# Keyspace
# Time:2019-08-02 10:56:55
kv keys:27512
hash keys:0
list keys:0
zset keys:0
set keys:0`