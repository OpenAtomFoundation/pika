Pika
====

Pika is redis like database.

Pika used Nemo as storage engine, Nemo is developed base on rocksdb.

Nemo support multi data structure by using rocksdb's kv port. Such as list,
hash, zst

Because rocksdb is using disk, so Nemo don't have capacity limitation.

The protocol used to contact with Pika is redis protocol, so you don't need to
change your code to migrate to Pika

### Thread Model
Pika used multi threads model. The main thread accept the request and then send
the request to the work threads.

Pika written all in C++

Just for fun ^-^
