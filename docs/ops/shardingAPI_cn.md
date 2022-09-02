## Pika自版本3.1.2起开始对分片做了一系列支持，为此我们为分片模式添加了一系列的命令.

在Pika分片版本我们引入了Table的概念，一个Table下面可以有若干个slot(slot的数量受配置文件中default-slot-num限制)客户端过来的读写请求会根据Key进行映射，如果该pika负责这个slot，该key会打到对应的slot上执行.

用分片模式启动，Pika会为我们创建一个默认的Table(名称为db0)，用户可以执行addslots命令向这个Table上增加slot，Pika会将Table对应的slot信息记录到db-path目录下的meta文件里.

不同Pika实例之间的slot是可以同步数据的，slot的身份可以是主可以是从，也可以既是主也是从，一旦slot有从的身份那么就是不可写的.

### 1.`pkcluster info`命令：
作用:用于展示slot的同步信息(包括Binlog偏移量，主从身份等)

`pkcluster info slot`: 查看默认table中所有slot的同步信息

`pkcluster info slot db0:0-6,7,8`: 查看table0下ID为0-6,7,8对应slot的同步信息

`pkcluster info table 1`: 查看table1的信息，包括QPS，table分片个数等信息。


### 2.`pkcluster addslots`命令：

作用:在默认table中添加指定ID的slot，ID的区间为[0，default-slot-num - 1]，支持以下三种指定ID的语法

`pkcluster addslots 0-2`: 在默认的table中添加id为0,1,2的三个slot

`pkcluster addslots 0-2,3`: 在默认的table中添加id为0,1,2,3的四个slot

`pkcluster addslots 0,1,2,3,4`: 在默认的table中添加id为0,1,2,3,4的五个slot


### 3.`pkcluster delslots`命令：

作用:在默认table中删除指定ID的slot，ID的区间为[0，default-slot-num - 1]，支持以下三种指定ID的语法

`pkcluster delslots 0-2`: 在默认的table中删除id为0,1,2的三个slot

`pkcluster delslots 0-2,3`: 在默认的table中删除id为0,1,2,3的四个slot

`pkcluster delslots 0,1,2,3,4`: 在默认的table中删除id为0,1,2,3,4的五个slot


### 4.`pkcluster slotsslaveof`命令：

作用:用于默认table下某些slot向其他Pika实例对应slot发起同步数据请求或者取消同步请求，指定slot的语法和上面addslots/delslots类似，但是这个命令中还支持使用all，表示默认table下的所有slot

`pkcluster slotsslaveof no one  [0-3,8-11 | all]` 指定的slot断开主从同步

`pkcluster slotsslaveof ip port [0-3,8,9,10,11 | all]` 指定的slot建立主从同步

`pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force` 指定的slot进行全同步

### 5.`slaveof` 命令

等价于 `pkcluster slotsslave ip port all` 命令

## 自pika3.3开始，分片模式支持动态创建table的功能。为了保持与原命令的兼容性和减少对多table不使用者的学习成本，pika默认会自动创建table 0，slot num为配置文件中的配置。使用其他table时，需要手动创建。
### 1. `pkcluster addtable` 命令：

作用：用于创建table，创建时需指定table-id,max-slot-num。默认table-id为0。

`pkcluster addtable 1 64`:创建table-id为1，max-slot-num为64的表。

### 2. `pkcluster deltalbe` 命令：

作用：用于删除table-id的表，并删除表中所有slot

` pkcluster deltable 1` :删除table-id 为1的表，并删除表中所有slot。

### 3.`pkcluster addslots`命令：

作用:在table-id的表中中添加指定ID的slot，ID的区间为[0，max-slot-num - 1]，支持以下三种指定ID的语法.不指定table-id时在默认表中添加。

`pkcluster addslots 0-2 1`: 在table-id为1的表中添加id为0,1,2的三个slot

`pkcluster addslots 0-2,3 1`: 在table-id为1的表中添加id为0,1,2,3的四个slot

`pkcluster addslots 0,1,2,3,4 1`: 在table-id为1的表中添加id为0,1,2,3,4的五个slot

### 4.`pkcluster delslots`命令：

作用:在table-id的表中删除指定ID的slot，ID的区间为[0，max-slot-num - 1]，支持以下三种指定ID的语法

`pkcluster delslots 0-2 1`: 在table-id为1的表中删除id为0,1,2的三个slot

`pkcluster delslots 0-2,3 1`: 在table-id为1的表中删除id为0,1,2,3的四个slot

`pkcluster delslots 0,1,2,3,4 1`: 在table-id为1的表中删除id为0,1,2,3,4的五个slot

### 5.`pkcluster slotsslaveof`命令：

作用:用于table-id的表下某些slot向其他Pika实例对应slot发起同步数据请求或者取消同步请求，指定slot的语法和上面addslots/delslots类似，但是这个命令中还支持使用all，表示table-id下的所有slot

`pkcluster slotsslaveof no one  [0-3,8-11 | all] 1 ` 指定table-id为1的表中slot断开主从同步

`pkcluster slotsslaveof ip port [0-3,8,9,10,11 | all] 1` 指定table-id为1的表中slot建立主从同步

`pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force 1` 指定table-id为1的表中slot进行全同步


<br/>

## 注意：
### 在分片模式下，pika全面支持输入参数为单个key的命令。
### 但对于输入参数可以是多个key的命令，在分片模式下进行了部分支持
<br/>

#### 目前分片模式不支持的命令

| —           | —                 | —            |
| ----------- | ----------------- | ------------ |
| Msetnx      | Scan              | Keys         |
| Scanx       | PKScanRange       | PKRScanRange |
| RPopLPush   | ZUnionstore       | ZInterstore  |
| SUnion      | SUnionstore       | SInter       |
| SInterstore | SDiff             | SDiffstore   |
| SMove       | BitOp             | PfAdd        |
| PfCount     | PfMerge           | GeoAdd       |
| GeoPos      | GeoDist           | GeoHash      |
| GeoRadius   | GeoRadiusByMember |              |

#### 分片模式命令支持计划
基础设施：支持[hash tags](https://redis.io/topics/cluster-spec)数据分片。用户可以针对具体的使用场景，一定程度上控制数据在集群中的分布。

基本方针：针对涉及多个Key的命令，Keys需要位于同一分片上，即只支持分片内的操作。而全局空间的命令将暂不支持。

例如：支持'RPOPLPUSH src dst'，需要src和dst列表位于同一分片上，而用户可以通过加入hash tag的方式达到该目的。
