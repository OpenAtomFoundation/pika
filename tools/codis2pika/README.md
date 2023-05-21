# codis2pika

[中文文档](https://github.com/GetuiLaboratory/codis2pika/blob/main/README_zh.md)

Codis2pika is a tool used to migrate codis data to pika. The main purpose is to support the migration of Codis sharding mode to Pika classic mode.

## 感谢

codis2pika 参考借鉴了阿里开源的redis-shake项目，并进行了定制化的改造。因此基本的功能特性与原工具一致，但是功能上存在差异。


## Features

Same features as the original:

* 🤗 Support the use of lua custom filtering rules (this part has not been changed, so there is no actual test, but it is theoretically supported.
* 💪 Support large instance migration.

Some features of codis2pika：
* 🌐 Support the source side as a stand-alone instance and the destination side as a stand-alone instance.
* 🌲 Only five basic data structures of Redis are supported.
* ✅ Testing on the Codis server based on Redis 3.2.
* ⏰ Support long time real-time data synchronization with several seconds delay.
* ✊ Not sensitive to the underlying storage mode of the instance.


### Description of changes
* Clustering is not supported: because the data is distributed differently at the bottom of the source instance (codis sharding mode) and the target instance (pika class mode), it needs to be allocated according to the actual business situation. If necessary, add a corresponding algorithm to restore the cluster write interface.
* If Redis migration is required, it is recommended to use [RedisShark](https://github.com/alibaba/RedisShake) tool for more comprehensive functions. This project is mainly to support the migration of sharding mode instances to pika classic instances.

# Document

## install

### Binary package

Release: [https://github.com/GetuiLaboratory/codis2pika/releases](https://github.com/GetuiLaboratory/codis2pika/releases)

### Compile from source

After downloading the source code, run the `sh build. sh` command to compile.

```shell
sh build.sh
```

## Usage

1. Edit codis2pika.toml, modify the source and target configuration items.
2. Start codis2pika:

```shell
./bin/codis2pika codis2pika.toml
```

3. Check data synchronization status.

## Configure

The codis2pika configuration file refers to `codis2pika. toml`. To avoid ambiguity, it is mandatory to assign values to each configuration in the configuration file, otherwise an error will be reported.

## Data filtering

codis2pika supports custom filtering rules using lua scripts. codis2pika can be started with the following command:

```shell
./bin/codis2pika codis2pika.toml filter/xxx.lua
```
However, the lua data filtering function has not been verified. Please refer to the redis shark project if necessary.

## Attention
* Extra large keys are not supported;
* The migrated codis needs to set the client output buffer limit to release the restriction, otherwise the link will be broken;
* The migrated node needs to reserve memory redundancy of the same amount of migrated data;
* Before and after data migration, it is recommended to `compact`;
* Multi db is not supported (I don't think it is necessary, and it is not supported for the time being);
* The expiration time of some keys may be delayed.

## Visualization
It is recommended to configure the monitoring disk in advance to have a visual grasp of the migration process.

## Verify
Alibaba open-source redis-full-check is recommended.
