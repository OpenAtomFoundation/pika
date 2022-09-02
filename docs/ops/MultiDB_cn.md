## Pika自版本3.1.0起开始支持多db，为了兼容多db部分命令、配置参数发生了变化，具体变化如下：

### 1.`info keyspace`命令：

**保留**：

`info keyspace [1|0]`:触发统计并展示、仅展示所有db的key信息

**新增：**

`info keyspace [1|0] db0`:触发统计并展示、仅展示db0的key信息

`info keyspace [1|0] db0,db2`:触发统计并展示、仅展示db0和db2的key信息

注意：db-name仅允许使用`db[0-7]`来表示，多个db-name使用`逗号`隔开



## 2.`compact`命令：

**保留：**

`compact`:对所有db进行compact

`compact [string/hash/set/zset/list/all]`:对所有db的某个数据结构、所有数据结构进行compact

**新增：**

`compact db0 all`:仅对db0的所有数据结构进行compact

`compact db0,db2 all`:对db0及db2的所有数据结构进行compact

`compact db1 string`:仅对db1的string数据结构进行compact

`compact db1,db3 hash`:对db1及db3的hash数据结构进行compact

注意：db-name仅允许使用`db[0-7]`来表示，多个db-name使用`逗号`隔开


## 3.`slaveof`命令:

**保留:**

`slaveof 192.168.1.1 6236 [force]`:为pika实例创建同步关系，影响所有db，可通过force参数进行实例级全量同步

**删除:**

`slaveof 192.168.1.1 6236 1234 111222333`:全局创建同步关系时不再允许指定write2file文件号、write2file文件偏移量


## 4.`bgsave`命令:

**保留:**

`bgsave`:对所有db进行快照式备份

**新增:**

`bgsave db0`:仅备份db0

`bgsave db0,db3`:仅备份db0及db3

注意：db-name仅允许使用`db[0-7]`来表示，多个db-name使用`逗号`隔开


## 5.`purgelogsto`命令:

**保留:**

`purgelogsto write2file1000`:删除db0中的write2file1000以前的所有write2file

**新增:**

`purgelogsto write2file1000 db1`:删除db1中的write2file1000以前的所有write2file，每次仅允许操作一个db

注意：db-name仅允许使用`db[0-7]`来表示


## 6.`flushdb`命令:

**保留:**

`flushdb [string/hash/set/zset/list]`:删除某个db中的某个数据结构

**新增:**

`flushdb`:删除某个db中的所有数据结构

注意:与redis一致，在pika中执行flushdb前请先select到准确的db，以防误删数据


## 7.`dbslaveof`命令:

`dbslaveof db[0 ~ 7]`: 同步某一个db

`dbslaveof db[0 ~ 7] force`: 全量同步某一个db

`dbslaveof db[0 ~ 7] no one`: 停止同步某一个db

`dbslaveof db[0 ~ 7] filenum offset`: 指定偏移量同步某一个db

注意:该命令需要在两个Pika实例已经建立了主从关系之后才能对单个db的同步状态进行控制
