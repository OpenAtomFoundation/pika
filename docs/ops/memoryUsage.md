### Pika内存占用
1. rocksdb 内存占用

#### 1. rocksdb 内存占用 
命令行命令 info data 

used_memory_human = db_memtable_usage + db_tablereader_usage

相应配置及对应影响参数

write-buffer-size          => db_memtable_usage

max-write-buffer-size      => db_memtable_usage

max-cache-files            => db_tablereader_usage

对应rocksdb配置解释

https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning

https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB

