Pika is a database that has the same feature as redis. The reason why we develop
Pika is to solve redis's memory limitation

Pika used Nemo as storage engine, Nemo is developed base on rocksdb.

Nemo support multi data structure by using rocksdb's kv port. Such as list,
hash, zset

Because rocksdb is using disk, so Nemo don't have capacity limitation.

The protocol used to contact with Pika is redis protocol, so you don't need to
change your code to migrate to Pika

### Thread Model
Pika used multi threads model. The main thread accept the request and then send
the request to the work threads.

Pika written all in C++

Just for fun ^-^
