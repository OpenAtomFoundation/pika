# Pika

## 简介 [English](https://github.com/Qihoo360/pika/blob/master/README.md)
Pika是一个可持久化的大容量redis存储服务，兼容string、hash、list、zset、set的绝大部分接口([兼容详情](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况))，解决redis由于存储数据量巨大而导致内存不够用的容量瓶颈，并且可以像redis一样，通过slaveof命令进行主从备份，支持全同步和部分同步，pika还可以用在twemproxy或者codis中来实现静态数据分片（pika已经可以支持codis的动态迁移slot功能，目前已合并到master分支，欢迎使用，感谢作者[left2right](https://github.com/left2right)和[fancy-rabbit](https://github.com/fancy-rabbit)提交的pr）

## Pika用户

<table>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/dcHpCm4.png" alt="Qihoo"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/BIjqe9R.jpg" alt="360game"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/jjZczkN.png" alt="Weibo"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/zoel46r.gif" alt="Garena"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/kHqACbn.png" alt="Apus"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/2c57z8U.png" alt="Ffan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/rUiO5VU.png" alt="Meituan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/px5mEuW.png" alt="XES"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://imgur.com/yJe4FP8.png" alt="HX"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/o8ZDXCH.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://imgur.com/w3qNQ9T.png" alt="GWD"></td>
<td height = "100" width = "150"><img src="https://imgur.com/KMVr3Z6.png" alt="DYD"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/vJbAfri.png" alt="YM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/aNxzwsY.png" alt="XM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/mrWxwkF.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://imgur.com/0oaVKlk.png" alt="YM"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="https://i.imgur.com/PI89mec.png" alt="MM"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/G9MOvZe.jpg" alt="VIP"></td>
<td height = "100" width = "150"><img src="https://imgur.com/vQW5qr3.png" alt="LK"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/jIMG4mi.jpg" alt="KS"></td>
</tr>
</table>


[更多](https://github.com/Qihoo360/pika/blob/master/USERS.md)

## 特点
* 容量大，支持百G数据量的存储
* 兼容redis，不用修改代码即可平滑从redis迁移到pika
* 支持主从(slaveof)
* 完善的[运维](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明)命令


## 使用

### 二进制包使用

用户可以直接从[releases](https://github.com/Qihoo360/pika/releases)下载最新的二进制版本包使用.

### 编译

#### 支持的平台

* linux - CentOS

* linux - Ubuntu

* macOS(Darwin)

#### 依赖软件
* gcc g++ 支持C++17 （version>=7）
* make
* cmake（version>=3.18）
* autoconf
* tar

#### 编译

1. 获取源代码

```
  git clone https://github.com/OpenAtomFoundation/pika.git
```

2. 切换到最新release版本

```
  a. 执行 `git tag` 查看最新的release tag，（如 v3.4.1）
  b. 执行 `git checkout TAG` 切换到最新版本，（如 git checkout v3.4.1）
```

3. 编译

如果在CentOS6，centOS7等 gcc 版本小于7的机器上，需要先升级gcc版本

执行如下
```
  a. sudo yum -y install centos-release-scl
  b. sudo yum -y install devtoolset-7-gcc devtoolset-7-gcc-c++
  c. scl enable devtoolset-7 bash
```

第一次编译时，建议使用构建脚本`build.sh`，该脚本会检查本机上是否有编译所需的软件
```
  ./build.sh
```

编译后的文件在`output`目录下

pika 默认使用`release`模式编译，不能调试，如果需要调试，需要使用`debug`模式来编译

```
  rm -fr output
  cmake -B output -DCMAKE_BUILD_TYPE=Debug
  cd output && make
```

## 使用
```
  ./output/pika -c ./conf/pika.conf
```

## 清空编译


  如果需要清空编译内容，视不同情况使用以下两种方法其一：
```
  1. 执行 cd output && make clean来清空pika的编译内容
  2. 执行 rm -fr output 重新生成cmkae（一般用于彻底重新编译）
```

## 容器化

### 使用docker运行

```bash
docker run -d \
  --restart=always \
  -p 9221:9221 \
  -v <log_dir>:/pika/log \
  -v <db_dir>:/pika/db \
  -v <dump_dir>:/pika/dump \
  -v <dbsync_dir>:/pika/dbsync \
  pikadb/pika:v3.3.6

redis-cli -p 9221 "info"
```

### 构建镜像
如果你想自己构建镜像，我们提供了一个脚本 `build_docker.sh` 来简化这个过程。

脚本接受几个可选参数：

- `-t tag`: 指定镜像的Docker标签。默认情况下，标签是 `pikadb/pika:<git tag>`。
- `-p platform`: 指定Docker镜像的平台。选项有 `all`, `linux/amd64`, `linux/arm`, `linux/arm64`，默认使用当前 docker 的 platform 设置。
- `--proxy`: 使用代理下载 package 以加快构建过程，构建时会使用阿里云的镜像源。
- `--help`: 显示帮助信息。

这是脚本的一个示例使用：

```bash
./build_docker.sh -p linux/amd64 -t private_registry/pika:latest
```

### 使用 pika-operator 部署

使用 `pika-operator` 可以简单地在 Kubernetes 环境中部署单实例 `pika` 。
请勿在生产环境中使用此功能。

本地安装：

1. 安装 [MiniKube](https://minikube.sigs.k8s.io/docs/start/)
2. 部署 pika-operator
```bash
cd tools/pika_operator
make minikube-up # run this if you don't have a minikube cluster
make local-deploy
```
3. 创建 pika 实例
```
cd tools/pika_operator
kubectl apply -f examples/pika-sample/

# check pika status
kubectl get pika pika-sample

# get pika instance info
kubectl run pika-sample-test --image redis -it --rm --restart=Never \
  -- /usr/local/bin/redis-cli -h pika-sample -p 9221 info
```

## 性能 (感谢[deep011](https://github.com/deep011)提供性能测试结果)
### 注!!!
本测试结果是在特定环境特定场景下得出的，不能够代表所有环境及场景下的表现，__仅供参考__。

__推荐大家在使用pika前在自己的环境根据自己的使用场景详细测试以评估pika是否满足要求__

### 测试环境

**CPU型号**：Intel(R) Xeon(R) CPU E5-2690 v4 @ 2.60GHz

**CPU线程数**：56

**MEMORY**：256G

**DISK**：3T flash

**NETWORK**：10GBase-T/Full * 2

**OS**：centos 6.6

**Pika版本**：2.2.4

### 压测工具

[**vire-benchmark**](https://deep011.github.io/vire-benchmark)

### 测试一

#### 测试目的

测试在pika不同worker线程数量下，其QPS上限。

#### 测试条件

pika数据容量：800G

value：128字节

CPU未绑定

#### 测试结果

说明：横轴Pika线程数，纵轴QPS，value为128字节。set3/get7代表30%的set和70%的get。

<img src="https://deep011.github.io/public/images/pika_benchmark/pika_threads_test.png" height = "60%" width = "60%" alt="1"/>

#### 结论

从以上测试图可以看出，pika的worker线程数设置为20-24比较划算。

### 测试二

#### 测试目的

测试在最佳worker线程数（20线程）下，pika的rtt表现。

#### 测试条件

**pika数据容量**：800G

**value**：128字节

#### 测试结果

```c
====== GET ======
  10000000 requests completed in 23.10 seconds
  200 parallel clients
  3 bytes payload
  keep alive: 1
99.89% <= 1 milliseconds
100.00% <= 2 milliseconds
100.00% <= 3 milliseconds
100.00% <= 5 milliseconds
100.00% <= 6 milliseconds
100.00% <= 7 milliseconds
100.00% <= 7 milliseconds
432862.97 requests per second
```

```c
====== SET ======
  10000000 requests completed in 36.15 seconds
  200 parallel clients
  3 bytes payload
  keep alive: 1
91.97% <= 1 milliseconds
99.98% <= 2 milliseconds
99.98% <= 3 milliseconds
99.98% <= 4 milliseconds
99.98% <= 5 milliseconds
99.98% <= 6 milliseconds
99.98% <= 7 milliseconds
99.98% <= 9 milliseconds
99.98% <= 10 milliseconds
99.98% <= 11 milliseconds
99.98% <= 12 milliseconds
99.98% <= 13 milliseconds
99.98% <= 16 milliseconds
99.98% <= 18 milliseconds
99.99% <= 19 milliseconds
99.99% <= 23 milliseconds
99.99% <= 24 milliseconds
99.99% <= 25 milliseconds
99.99% <= 27 milliseconds
99.99% <= 28 milliseconds
99.99% <= 34 milliseconds
99.99% <= 37 milliseconds
99.99% <= 39 milliseconds
99.99% <= 40 milliseconds
99.99% <= 46 milliseconds
99.99% <= 48 milliseconds
99.99% <= 49 milliseconds
99.99% <= 50 milliseconds
99.99% <= 51 milliseconds
99.99% <= 52 milliseconds
99.99% <= 61 milliseconds
99.99% <= 63 milliseconds
99.99% <= 72 milliseconds
99.99% <= 73 milliseconds
99.99% <= 74 milliseconds
99.99% <= 76 milliseconds
99.99% <= 83 milliseconds
99.99% <= 84 milliseconds
99.99% <= 88 milliseconds
99.99% <= 89 milliseconds
99.99% <= 133 milliseconds
99.99% <= 134 milliseconds
99.99% <= 146 milliseconds
99.99% <= 147 milliseconds
100.00% <= 203 milliseconds
100.00% <= 204 milliseconds
100.00% <= 208 milliseconds
100.00% <= 217 milliseconds
100.00% <= 218 milliseconds
100.00% <= 219 milliseconds
100.00% <= 220 milliseconds
100.00% <= 229 milliseconds
100.00% <= 229 milliseconds
276617.50 requests per second
```

#### 结论

get/set 响应时间 99.9%都在2ms以内。

### 测试三

#### 测试目的

在pika最佳的worker线程数下，查看各命令的极限QPS。

#### 测试条件

**pika的worker线程数**：20

**key数量**：10000

**field数量**：100（list除外）

**value**：128字节

**命令执行次数**：1000万（lrange除外）

#### 测试结果

```c
PING_INLINE: 548606.50 requests per second
PING_BULK: 544573.31 requests per second
SET: 231830.31 requests per second
GET: 512163.91 requests per second
INCR: 230861.56 requests per second
MSET (10 keys): 94991.12 requests per second
LPUSH: 196093.81 requests per second
RPUSH: 195186.69 requests per second
LPOP: 131156.14 requests per second
RPOP: 152292.77 requests per second
LPUSH (needed to benchmark LRANGE): 196734.20 requests per second
LRANGE_10 (first 10 elements): 334448.16 requests per second
LRANGE_100 (first 100 elements): 50705.12 requests per second
LRANGE_300 (first 300 elements): 16745.16 requests per second
LRANGE_450 (first 450 elements): 6787.94 requests per second
LRANGE_600 (first 600 elements): 3170.38 requests per second
SADD: 160885.52 requests per second
SPOP: 128920.80 requests per second
HSET: 180209.41 requests per second
HINCRBY: 153364.81 requests per second
HINCRBYFLOAT: 141095.47 requests per second
HGET: 506791.00 requests per second
HMSET (10 fields): 27777.31 requests per second
HMGET (10 fields): 38998.52 requests per second
HGETALL: 109059.58 requests per second
ZADD: 120583.62 requests per second
ZREM: 161689.33 requests per second
PFADD: 6153.47 requests per second
PFCOUNT: 28312.57 requests per second
PFADD (needed to benchmark PFMERGE): 6166.37 requests per second
PFMERGE: 6007.09 requests per second
```

#### 结论

整体表现很不错，个别命令表现较弱（LRANGE，PFADD，PFMERGE）。

### 测试四

#### 测试目的

Pika与Redis的极限QPS对比。

#### 测试条件

**pika的worker线程数**：20

**key数量**：10000

**field数量**：100（list除外）

**value**：128字节

**命令执行次数**：1000万（lrange除外）

**Redis版本**：3.2.0

#### 测试结果

<img src="https://deep011.github.io/public/images/pika_benchmark/pika_vs_redis_qps.png" height = "60%" width = "60%" alt="1"/>

## 可观测性

### Metrics

1. Pika Server Info：系统架构、IP、端口、run_id、配置文件等
2. Pika Data Info：DB 大小、日志大小、内存使用情况等
3. Pika ClientsInfo：连接的客户端
4. Pika Stats Info：compact、slot等状态信息
5. Pika Network Info：客户端和主从复制的传入和传出流量以及速率
6. Pika CPU Info：CPU使用情况
7. Pika Replication Info：主从复制的状态信息，binlog 信息等
8. Pika Keyspace Info：五种数据类型的 Key 信息
9. Pika Command Exec Count Info：命令执行计数
10. Pika Command Execution Time：命令执行耗时
11. RocksDB Metrics：五种数据类型的 RocksDB 信息，包括 Memtable、Block Cache、Compaction、SST File、Blob File 等。

详细请参考 [指标 Metrics](tools/pika_exporter/README.md)。

## 文档
1. [doc](https://github.com/OpenAtomFoundation/pika/wiki)

## 联系方式

![](docs/images/pika-wechat-cn.png)

* [Slack Channel](https://join.slack.com/t/w1687838400-twm937235/shared_invite/zt-1y72dch5d-~9CuERHYUSmfeJZh32Z~qQ)

* QQ群：294254078
