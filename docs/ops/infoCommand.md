执行`INFO`命令

```
127.0.0.1:9221> info [section]
```

```
#主库：
# Server
pika_version:2.3.0 -------------------------------------------- pika 版本信息
pika_git_sha:3668a2807a3d047ea43656b58a2130c1566eeb65 --------- git的sha值
pika_build_compile_date: Nov 14 2017 -------------------------- pika的编译日期
os:Linux 2.6.32-2.0.0.8-6 x86_64 ------------------------------ 操作系统信息
arch_bits:64 -------------------------------------------------- 操作系统位数
process_id:12969 ---------------------------------------------- pika pid信息
tcp_port:9001 ------------------------------------------------- pika 端口信息
thread_num:12 ------------------------------------------------- pika 线程数量
sync_thread_num:6 --------------------------------------------- sync线程数量
uptime_in_seconds:3074 ---------------------------------------- pika 运行时间（秒）
uptime_in_days:0 ---------------------------------------------- pika 运行时间（天）
config_file:/data1/pika9001/pika9001.conf --------------------- pika conf文件位置
server_id:1 --------------------------------------------------- pika的server id

# Data
db_size:770439 ------------------------------------------------ db的大小(Byte)
db_size_human:0M ---------------------------------------------- 人类可读的db大小(M)
compression:snappy -------------------------------------------- 压缩方式
used_memory:4248 ---------------------------------------------- 使用的内存大小(Byte)
used_memory_human:0M ------------------------------------------ 人类可读的使用的内存大小(M)
db_memtable_usage:4120 ---------------------------------------- memtable的使用量(Byte)
db_tablereader_usage:128 -------------------------------------- tablereader的使用量(Byte)

# Log
log_size:110174 ----------------------------------------------- binlog的大小(Byte)
log_size_human:0M --------------------------------------------- 人类可读的binlog大小(M)
safety_purge:none --------------------------------------------- 目前可以安全删除的文件名
expire_logs_days:7 -------------------------------------------- 设置binlog过期天数
expire_logs_nums:10 ------------------------------------------- 设置binlog过期数量
binlog_offset:0 388 ------------------------------------------- binlog偏移量(文件号，偏移量)
 
# Clients
connected_clients:2 ------------------------------------------- 当前连接数
 
# Stats
total_connections_received:18 --------------------------------- 总连接次数统计
instantaneous_ops_per_sec:1 ----------------------------------- 当前qps
total_commands_processed:633 ---------------------------------- 请求总计
is_bgsaving:No, , 0 ------------------------------------------- pika 备份信息：是否在备份,备份名称，备份
is_scaning_keyspace:No ---------------------------------------- 是否在执行scan操作
is_compact:No ------------------------------------------------- 是否在执行数据压缩操作
compact_cron: ------------------------------------------------- 定时compact(format: start-end/ratio, eg. 02-04/60)
compact_interval: --------------------------------------------- compact的间隔(format: interval/ratio, eg. 6/60)

# CPU
used_cpu_sys:48.52 -------------------------------------------- Pika进程系统CPU占用时间
used_cpu_user:73.10 ------------------------------------------- Pika进程用户CPU占用时间
used_cpu_sys_children:0.05 ------------------------------------ Pika子进程系统CPU占用时间
used_cpu_user_children:0.05 ----------------------------------- Pika子进程用户CPU占用时间
 
# Replication(MASTER)
role:master --------------------------------------------------- 本实例角色
connected_slaves:1 -------------------------------------------- 当前从库数量
slave0:ip=192.168.1.1,port=57765,state=online,sid=2,lag=0 ----- lag：当前主从binlog相差的字节数（byte），如果有多个从库则依次展示
 
#从库（区别仅在于同步信息的展示）：
# Replication(SLAVE)
role:slave ---------------------------------------------------- 本实例角色
master_host:192.168.1.2 --------------------------------------- 主库IP
master_port:9001 ---------------------------------------------- 主库端口
master_link_status:up ----------------------------------------- 当前同步状态
slave_read_only:1 --------------------------------------------- 从库是否readonly
repl_state: connected ----------------------------------------- 从库同步连接的当前状态
 
# Keyspace（key数量展示，按照数据类型分类展示，默认不更新，仅在执行info keyspace 1的时候刷新该信息）
# Time:2016-04-22 17:08:33 ------------------------------------ 上一次统计的时间
db0 Strings_keys=100004, expires=0, invaild_keys=0
db0 Hashes_keys=2, expires=0, invaild_keys=0
db0 Lists_keys=0, expires=0, invaild_keys=0
db0 Zsets_keys=1, expires=0, invaild_keys=0
db0 Sets_keys=0, expires=0, invaild_keys=0
# keys：当前有效KEY的数量，等同于redis的keys 
# expires：keys中带有expire属性的key的数量，等同于Redis 
# invalid_keys：pika独有，指已经失效（标记删除），但还未被rocksdb彻底物理删除的key，虽然这些key不再会被访问到，但会占用一定磁盘空间，如果发现较大可以通过执行compact来彻底清理

# DoubleMaster(MASTER)
role:master --------------------------------------------------- 双主角色
the peer-master host: ----------------------------------------- 双主对端IP
the peer-master port:0 ---------------------------------------- 双主对端Port
the peer-master server_id:0 ----------------------------------- 双主对端server id
double_master_mode: False ------------------------------------- 是否配置双主模式
repl_state: 0 ------------------------------------------------- 双主连接状态
double_master_recv_info: filenum 0 offset 0 ------------------- 从对端接受的Binlog偏移量
```
