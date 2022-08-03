# 全量同步
## 背景
### 1.Pika Replicate
- pika支持master/slave的复制方式，通过slave端的slaveof命令激发
- salve端处理slaveof命令，将当前状态变为slave，改变连接状态
- slave的向master发送MetaSync请求，在同步之前确保自身db的拓扑结构和master一致
- slave下的每个partition单独的向master端对应的partition发起trysync请求，建立同步关系
### 2.Binlog
- pika同步依赖binlog
- binlog文件会自动或手动删除
- 当同步点对应的binlog文件不存在时，需要通过全同步进行数据同步
## 全同步
### 1. 简介
- 需要进行全同步时，master会将db文件dump后发送给slave
- 通过rsync的deamon模式实现db文件的传输
- 默认使用pika port+1000作为rysnc传输端口
### 2. 实现逻辑
1. 在pika实例启动的同时会启动Rsync服务
2. master发现某一个partition需要全同步时，判断是否有备份文件可用，如果没有先dump一份
3. master通过rsync向slave发送对应partition的dump的文件
4. slave的对应partition用收到的文件替换自己的db
5. slave的对应partition用最新的偏移量再次发起trysnc
6. 完成同步

Slave中某一个Partition建立同步:
![slave的partition](https://i.imgur.com/flnOyeZ.png)

Master处理同步请求：
![master执行过程](https://i.imgur.com/Beclo9c.png)
### 3. Slave连接状态
- No Connect：不尝试成为任何其他节点的slave
- ShouldMetaSync：向master请求db的拓扑信息，确保与自身一致
- TryConnect：为每个partition重置状态机，让其处于准备同步的状态
- Connecting：在所有partition没有建立同步关系之前一直是处于connecting的状态
- EstablishSucces: 所有partition建立同步关系成功
- Error：出现了异常


# 增量同步

## 背景 ：
<br/></br>
从库Pika得到主库的全部DB结构，接下来以partition维度做Trysync，如果从库确认可以增量同步，从库将以partition为维度进行增量同步。默认使用pika port+2000的端口进行增量同步。

### Binlog 结构：
<br/></br>
Pika的主从同步是使用Binlog来完成的，一主多从的结构master节点也可以给多个slave复用一个Binlog，只不过不同的slave在binglog中有自己的偏移量而已，master执行完一条写命令就将命令追加到Binlog中，pika的同步模块会读出对应的binlog发送到slave，而slave收到binlog后会执行并追加到自己的Binlog中，由于主从偏移量一样，所以一旦发生网络或节点故障需要重连主从时，slave仅需要将自己当前的Binlog偏移量发送给master，master找到后从该偏移量开始同步后续命令，理论上将命令不做处理一条一条追加到文件中，但是这样的记录格式容错很差，如果读文件中写错一个字节则导致整个文件不可用，所以pika采用了类似leveldb log的格式来进行存储，具体如下：

![image](http://ww2.sinaimg.cn/large/c2cd4307gw1f6m74717b3j20rm0gjwgw.jpg)

### 交互过程：
<br/></br>
1，从库发送BinlogSyncRequest 报文，报文中需说明自己已经收到的BinlogOffset。

2，主库收到BinlogSyncRequest之后会从同步点开始发出一批BinlogSyncResponse。

3，从库在收到BinlogSyncResponse之后，会在写入本地binlog之后再进行1流程。

![image](https://i.imgur.com/JVfTV22.png)

## 同步模块:
<br/></br>
![image](https://i.imgur.com/5ByKpsA.png )

Pika的同步由ReplicaManager(RM)模块负责。RM中有两层结构，逻辑层负责同步逻辑，传输层负责链接的管理数据的解析和传输。

数据的同步的最小单位是Partition，每一个Pika实例会维护自己作为主的partition(MasterPartition)和自己是从的partition(SlavePartition)。对于MasterPartition，需要记录跟随自己的slave同步信息，逻辑层会根据该信息向slave同步信息。对于SlavePartition，则是需要需要记录master的信息，逻辑层会按照该信息按需向master发送同步请求。

逻辑层维护两个数据结构，一个是MasterPartitions，记录跟随自己的SlaveNode信息，主要包括slave的同步状态和当前的sessionId。另一个是SlavePartitions，记录主的信息。

传输层分为两个子模块，ReplicationClient负责发起链接的建立，ReplicationServer负责响应报文。每两个实例之间的所有partition复用一条链接。



## 同步过程:
<br/></br>
![image](https://i.imgur.com/1Q8PbjF.png )


### MasterPartition 同步事件
逻辑层处理MasterPartition的同步事件，向其对应的从同步数据。

1，读取MasterPartition Binlog信息后，将BinlogOffsetInfo记录到SlaveNode自己的window中。

2，将Binlog暂存到临时的待发送队列中。

3，辅助线程(Auxiliary thread) 定时将临时待发送队列的数据通过RM的传输层发送给对应的slave节点。

4，收到slave的BinlogSyncResponse之后，得知slave收到的BinlogOffset信息，更新SlaveNode window，重复1流程继续同步

为了控制每个SlaveNode同步的速度，避免某几个SlaveNode占用过多资源，为每一个SlaveNode设置了window。如下图所示，Pika收到了BinlogOffset为100到200的ack response，从window中移除BinlogOffset位于100到200的元素，之后继续发送BinlogOffset为1100和1200的binlog，对应的BinlogOffset添加至window中。



![image](https://i.imgur.com/0GtOhk4.png)


### SlavePartition 同步事件
逻辑层处理SlavePartition的同步事件，收到master发送的同步数据，向master发相应的response信息。

1，按照解析出的Partition信息，被分配到对应的线程处理binlog写入任务。

2，线程写入binlog之后，调用传输层发送BinlogSyncResponse信息。

3，根据binlog的key分配给对应的线程处理写入db任务。

