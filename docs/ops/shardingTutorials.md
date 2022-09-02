In sharding mode，pika have slot. Pika hash the request key, then mod the slots count, after that send the request to the slot. Sharding mode can be used in Singular and cluster. This tutorial will show the details. 

#### 0. Mode
  We provide two modes in Pika, one is classic mode which is the same as before, another is sharding mode. In sharding mode, the cluster provide 1024 slots as default, every slot have 5 data structures, you will have 1024*5 rocksdb instance, so enough physical servers you should prepare and you should carefully estimate.
  
[3.2.x Performance](https://github.com/Qihoo360/pika/wiki/3.2.x-Performance)。


#### 1. Version

  After v3.2.0

#### 2. Operation

  Slots details [slot commands](https://github.com/Qihoo360/pika/wiki/Pika分片命令)

#### 3. Configure


```
# default slot number each table in sharding mode
  default-slot-num : 1024

# if this option is set to 'classic', that means pika support multiple DB, in
# this mode, option databases enable
# if this option is set to 'sharding', that means pika support multiple Table, you
# can specify slot num for each table, in this mode, option default-slot-num enable
# Pika instance mode [classic | sharding]
  instance-mode : sharding

# Pika write-buffer-size
write-buffer-size : 67108864

# If the total size of all live memtables of all the DBs exceeds
# the limit, a flush will be triggered in the next DB to which the next write
# is issued.
max-write-buffer-size : 10737418240

# maximum value of Rocksdb cached open file descriptors
max-cache-files : 100
```
[Configure](https://github.com/Qihoo360/pika/wiki/pika-配置文件说明)

#### 4. codis，twemproxy compatibility

  Details [Support Cluster Slots](https://github.com/Qihoo360/pika/wiki/Support-Cluster-Slots)。