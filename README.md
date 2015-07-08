Pika
====

Pika is a simple nosql database.
Pika used leveldb as storage engine.
The protocol used to contact with Pika is redis protocol

### Thread Model
Pika used multi threads model. The main thread accept the request and then send
the request to the work threads.

Pika written all in C++

Just for fun ^-^
