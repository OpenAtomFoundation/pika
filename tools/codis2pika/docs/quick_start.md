# 快速开始

## 实例信息

### 实例 A

- 地址：codis-server-host:6379
- 端口：6379
- 密码：xxxxx

### pika 实例 B

- 地址：192.168.0.1:6379
- 端口：6379
- 密码：xxxxx

### 实例 C codis集群实例

- 地址：
    - codis-server-host-1:6379
    - codis-server-host-2:6379
- 密码：xxxxx

## 工作目录

```
.
├── codis2pika # 二进制程序
└── codis2pika.toml # 配置文件
```

## 开始

## A -> B 同步

修改 `codis2pika.toml`，改为如下配置：

```toml
[source]
type = "sync"
address = "codis-server-host:6379"
password = "xxxxx"

[target]
type = "standalone"
address = "192.168.0.1:6379"
password = "xxxxx"
```

启动 codis2pika：

```bash
./codis2pika codis2pika.toml
```

## C -> B 同步

修改 `codis2pika.toml`，改为如下配置：

```toml
[source]
type = "sync"
address = "codis-server-host-1:6379"
password = "xxxxx"

[target]
type = "standalone"
address = "192.168.0.1:6379" # 这里写集群中的任意一个节点的地址即可
password = "xxxxx"

[advanced]
dir = "data" #不可重复
```
修改第二个，`cp codis2pika.toml codis2pika2.toml`
```toml
[source]
type = "sync"
address = "codis-server-host-2:6379"
password = "xxxxx"

[target]
type = "standalone"
address = "192.168.0.1:6379" # 这里写集群中的任意一个节点的地址即可
password = "xxxxx"

[advanced]
dir = "data2" #不可重复
```

启动 codis2pika：

```bash
nohup ./codis2pika codis2pika.toml &
nohup ./codis2pika codis2pika2.toml &
```

同理，集群C也可以起多个，实现codis集群迁移到pika集群。