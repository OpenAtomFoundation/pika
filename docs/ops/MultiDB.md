## Multiple DB supported since Pika v3.1.0

### 1.`info keyspace`：

**Original**：

`info keyspace [1|0]`: all db keys information

**New Added**

`info keyspace [1|0] db0`: db0 keys information

`info keyspace [1|0] db0,db2`: db0 and db2 keys information

Notice, comma splited between dbs.



## 2.`compact`：

**Original**

`compact`:compact all in db

`compact [string/hash/set/zset/list/all]`:compact in data structure

**New Added**

`compact db0 all`:compact in db0

`compact db0,db2 all`:compact in db0 and db2

`compact db1 string`:compact in db1 string type

`compact db1,db3 hash`:compact in db1 and db3 hash type

Notice, comma splited between dbs.


## 3.`slaveof`:

**Original**

`slaveof 192.168.1.1 6236 [force]`:force full sync between instances.

**Delete:**

`slaveof 192.168.1.1 6236 1234 111222333`:write2file file and offset are not allow in full sync


## 4.`bgsave`:

**Original**

`bgsave`:backup all

**New Added**

`bgsave db0`:backup db0

`bgsave db0,db3`:backup db0 and db3

Notice, comma splited between dbs.


## 5.`purgelogsto`:

**Original**

`purgelogsto write2file1000`:delete write2file before write2file1000 in db0

**New Added**

`purgelogsto write2file1000 db1`:delete write2file before write2file1000 in db1

Notice, db-name range `db[0-7]`


## 6.`flushdb`:

**Original**

`flushdb [string/hash/set/zset/list]`:delete some data structure

**New Added**

`flushdb`:delete all data structure in some db

Notice, should select firstly before delete like Redis


## 7.`dbslaveof`:

`dbslaveof db[0 ~ 7]`: sync

`dbslaveof db[0 ~ 7] force`: force full suync

`dbslaveof db[0 ~ 7] no one`: stop sync from

`dbslaveof db[0 ~ 7] filenum offset`: sync with offset

