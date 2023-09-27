<img src="https://s1.ax1x.com/2020/05/08/YnbjQf.png" alt="YnbjQf.png"  width="300" />

[![Build Status](https://travis-ci.org/Qihoo360/pika.svg?branch=master)](https://travis-ci.org/Qihoo360/pika) ![Downloads](https://img.shields.io/github/downloads/Qihoo360/pika/total)

|                                             **Stargazers Over Time**                                              |                                                                                                            **Contributors Over Time**                                                                                                            |
|:-----------------------------------------------------------------------------------------------------------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      [![Stargazers over time](https://starchart.cc/OpenAtomFoundation/pika.svg)](https://starchart.cc/OpenAtomFoundation/pika)      | [![Contributor over time](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=OpenAtomFoundation/pika)](https://contributor-graph-api.apiseven.com/contributors-svg?chart=contributorOverTime&repo=OpenAtomFoundation/pika) |

## Introduction[中文](https://github.com/OpenAtomFoundation/pika/blob/unstable/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization. You can also use pika together with twemproxy or codis(*pika has supported data migration in codis，thanks [left2right](https://github.com/left2right) and [fancy-rabbit](https://github.com/fancy-rabbit)*) for distributed Redis solution


## UserList

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

[More](docs/USERS.md)

## Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明) interfaces

## For developer

### Releases
The User can download the binary release from [releases](https://github.com/Qihoo360/pika/releases) or compile the source release.

### Compile

#### Supported platforms

* linux - CentOS

* linux - Ubuntu

* macOS

#### Dependencies

* gcc g++, C++17 support (version>=7)
* make
* cmake (version>=3.18)
* autoconf
* tar


#### Compile

Upgrade your gcc to version at least 7 to get C++17 support.

1. Get the source code

```
  git clone https://github.com/OpenAtomFoundation/pika.git
```

2. Checkout the latest release version

```
  a. exec git tag to get the latest release tag
  b. exec git checkout TAG to switch to the latest version
```

3. Compile

Please run the script build.sh before you compile this db to check the environment and build this repo.
If the gcc version is later than 7, such as CentOS6 or centOS7, you need to upgrade the gcc version first

Do as follows
```
  a. sudo yum -y install centos-release-scl
  b. sudo yum -y install devtoolset-7-gcc devtoolset-7-gcc-c++
  c. scl enable devtoolset-7 bash
```


Please run the script build.sh before you compile this db to check the environment and build this repo.

```
  ./build.sh
```

The compilation result is in the 'output' directory.

By default the compilation process is in 'release' mode. If you wanna debug this db，you need to compile it in 'debug' mode.

```
  rm -fr output
  cmake -B output -DCMAKE_BUILD_TYPE=Debug
  cd output && make
```

## Usage

```
./output/pika -c ./conf/pika.conf
```

## Clean compilation

```
  If wanna clean up the compilation content, you can choose one of the following two methods as your will.
  1. exec `cd output && make clean` clean pika Compile content
  2. exec `rm -fr output` rebuild cmake (for complete recompilation)
```

## Developing and Debugging with Pika

- [Setting up the Development and Debugging Environment for Pika using CLion](./docs/ops/SetUpDevEnvironment.md)

## Dockerization

### Run with docker

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

Meaning of dirs:
- log_dir: Directory to store log files of Pika.
- db_dir: Directory to store the data of Pika.
- dump_dir: Directory to stored dump files that generated by command "bgsave".
- dbsync_dir: Pika db sync path.


### Build Image
If you want to build the image yourself, we have provided a script `build_docker.sh` to simplify this process.

The script accepts several optional arguments:

- `-t tag`: Specify the Docker tag for the image. By default, the tag is `pikadb/pika:<git tag>`.
- `-p platform`: Specify the platform for the Docker image. By default is current docker's platform. `all`, `linux/amd64`, `linux/arm`, `linux/arm64`.
- `--proxy`: Use a proxy to download packages to speed up the build process. This is particularly useful if you are in China.
- `--help`: Display help information.

Here is an example usage of the script:

```bash
./build_docker.sh -p linux/amd64 -t private_registry/pika:latest
```
### Use pika-operator to deploy

You can use the `pika-operator` to easily deploy `pika` in a Kubernetes environment. 

Please note that this operator is **NOT** recommended for use in a production environment.

Local Deploy：

1. install [MiniKube](https://minikube.sigs.k8s.io/docs/start/)
2. deploy pika-operator
```bash
cd tools/pika_operator
make minikube-up # run this if you don't have a minikube cluster
make local-deploy
```
3. create pika instance
```
cd tools/pika_operator
kubectl apply -f examples/pika-sample/

# check pika status
kubectl get pika pika-sample

# get pika instance info
kubectl run pika-sample-test --image redis -it --rm --restart=Never \
  -- /usr/local/bin/redis-cli -h pika-sample -p 9221 info
```

## Performance

More details on [Performance](docs/benchmark/performance.md).

## Observability

### Metrics

1. Pika Server Info: system, ip, port, run_id, config file etc.
2. Pika Data Info: db size, log size, memory usage etc.
3. Pika Client Info: The number of connected clients.
4. Pika Stats Info: status information of compact, slot, etc.
5. Pika Network Info: Incoming and outgoing traffic and rate of client and master-slave replication.
6. Pika CPU Info: cpu usage.
7. Pika Replication Info: Status information of master-slave replication, binlog information.
8. Pika Keyspace Info: key information of five data types.
9. Pika Command Exec Count Info: command execution count.
10. Pika Command Execution Time: Time-consuming command execution.
11. RocksDB Metrics: RocksDB information of five data types, includes Memtable, Block Cache, Compaction, SST File, Blob File etc.

More details on [Metrics](tools/pika_exporter/README.md).

## Documents

1. [doc](https://github.com/OpenAtomFoundation/pika/wiki)

## Contact Us

![](docs/images/pika-wechat.png)

* [Slack Channel](https://join.slack.com/t/w1687838400-twm937235/shared_invite/zt-1y72dch5d-~9CuERHYUSmfeJZh32Z~qQ)

QQ group: 294254078

