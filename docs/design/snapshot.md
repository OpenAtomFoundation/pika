# Pika 快照式备份方案

## 原理
不同于Redis，Pika的数据主要存储在磁盘中，这就使得其在做数据备份时有天然的优势，可以直接通过文件拷贝实现
实现

![](http://ww4.sinaimg.cn/large/c2cd4307gw1f6m745csxsj20fl0iojss.jpg)
 
## 流程
- 打快照：阻写(阻止客户端进行写db操作)，并在这个过程中获取快照内容
- 异步线程拷贝文件：通过修改Rocksdb提供的BackupEngine拷贝快照中文件，这个过程中会阻止文件的删除

## 快照内容
- 当前db的所有文件名
- manifest文件大小
- sequence_number
- 同步点
    - binlog filenum
    - offset