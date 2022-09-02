### Pika Memroy
1. rocksdb memory usage
2. pika memory usage(tcmalloc)

#### 1. rocksdb memory usage
Command: info data 

used_memory_human = db_memtable_usage + db_tablereader_usage

Configs:

write-buffer-size          => db_memtable_usage

max-write-buffer-size      => db_memtable_usage

max-cache-files            => db_tablereader_usage

Refer: 

https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning

https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB

#### 2. pika memory usage

Command: tcmalloc stats

Command: tcmalloc free