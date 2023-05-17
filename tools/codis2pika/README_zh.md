# codis2pika

codis2pika 是一个用来做 codis 数据迁移到 pika 的工具。主要目的是为了支持codis 分片模式迁移到 pika classic模式。

## 感谢

codis2pika 参考借鉴了阿里开源的redis-shake项目，并进行了定制化的改造。因此基本的功能特性与原工具一致，但是功能上存在差异。


## 特性

与原版相同的特性：

* 🤗 支持使用 lua 自定义过滤规则（这部分未作改动，因此没实际测试，但理论上是支持的）
* 💪 支持大实例迁移

codis2pika的一些特性：
* 🌐 支持源端为单机实例，目的端为单机实例
* 🌲 仅支持 Redis 5种基础数据结构
* ✅ 测试在 Redis 3.2 版本的codis server
* ⏰ 支持较长时间的数据实时同步，存在几秒延迟
* ✊ 对实例的底层存储模式不敏感

### 改动说明
* 不支持集群： 由于数据在源实例（codis sharding模式）与目标实例（pika classic模式）底层分布不同，需要结合业务实际情况分配。如有需要，可以在添加对应算法，恢复集群写入接口。
* 如果需要redis迁移的，建议还是用[RedisShark](https://github.com/alibaba/RedisShake)工具，功能更全面。本项目主要是为了支持sharding模式实例迁移到pika classic实例。

# 文档

## 安装

### 从 Release 下载安装

Release: [https://github.com/GetuiLaboratory/codis2pika/releases](https://github.com/GetuiLaboratory/codis2pika/releases)

### 从源码编译

下载源码后，运行 `sh build.sh` 命令编译。

```shell
sh build.sh
```

## 运行

1. 编辑 codis2pika.toml，修改其中的 source 与 target 配置项
2. 启动 codis2pika：

```shell
./bin/codis2pika codis2pika.toml
```

3. 观察数据同步情况

## 配置

codis2pika 配置文件参考 `codis2pika.toml`。 为避免歧义强制要求配置文件中的每一项配置均需要赋值，否则会报错。

## 数据过滤

codis2pika 支持使用 lua 脚本自定义过滤规则，可以实现对数据进行过滤。 搭配 lua 脚本时，codis2pika 启动命令：

```shell
./bin/codis2pika codis2pika.toml filter/xxx.lua
```
lua 数据过滤功能未作验证，如有需要请参考redis-shark项目

## 注意事项
* 不支持特大key;
* 被迁移的codis需要设置client-output-buffer-limit解除限制，否则会断开链接;
* 被迁移节点需要预留同等迁移数据量的内存冗余;
* 在执行数据迁移前后，建议进行compact;
* 不支持多db（感觉必要性不大，暂时没支持;
* 部分key的过期时间可能推后。

## 迁移过程可视化
推荐提前配置监控大盘，可以对迁移过程有可视化的掌握。

## 验证迁移结果

推荐使用阿里开源的redis-full-check工具

