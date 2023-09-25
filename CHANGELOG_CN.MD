# v3.5.1



## 新特性

- slow log 增加队列等待时间统计 [#1997](https://github.com/OpenAtomFoundation/pika/pull/1997) @[wangshao1](https://github.com/wangshao1)

- 主从复制使用 ReplicationID [#1951](https://github.com/OpenAtomFoundation/pika/pull/1951) @[Mixficsol](https://github.com/Mixficsol)

- WAL 以 disablewal 命令方式支持动态关闭 [#2015](https://github.com/OpenAtomFoundation/pika/pull/2015) @[Mixficsol](https://github.com/Mixficsol)

- flush 线程数和 compaction 线程数动态调整，合二为一 [#2014](flush线程数和compaction线程数动态调整，合二为一) @[Tianpingan](https://github.com/Tianpingan)

- 升级了 RocksDB 版本到 v8.3.3 [#1999](https://github.com/OpenAtomFoundation/pika/issues/1999) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- 增加了 Macos 环境下配置 Pika 的使用方法文档 [#2003](https://github.com/OpenAtomFoundation/pika/pull/2003) @[klboke](https://github.com/klboke)

- 新增周期性打印工作队列的长度功能，在队列阻塞的时候可以快速定位问题 [#1978](https://github.com/OpenAtomFoundation/pika/pull/1978) @[Tianpingan](https://github.com/Tianpingan)

- 新增利用一个 `pika_exporter` 检测整个集群的指标 [#1953](https://github.com/OpenAtomFoundation/pika/pull/1953) @[chenbt-hz](https://github.com/chenbt-hz)

- 实现在 K8s 环境上 Pika 服务的自动注册，在启动时自动注册，从而实现集群的自组织 [#1931](https://github.com/OpenAtomFoundation/pika/pull/1931) @[machinly](https://github.com/machinly)


## bugfix

- 减少了 exporter 非必要的日志打印，降低 CPU 利用率 [#1945](https://github.com/OpenAtomFoundation/pika/pull/1945) @[Mixficsol](https://github.com/Mixficsol)

- 升级了 sentinel 机制，对于掉线的主节点，进行逻辑删除 [#1949](https://github.com/OpenAtomFoundation/pika/pull/1949) @[Mixficsol](https://github.com/Mixficsol)

- 调整 rate_limit 参数，修复压测时出现 RPS 为 0 的情况 [#2009](https://github.com/OpenAtomFoundation/pika/pull/2009) @[chejinge](https://github.com/chejinge)

- 修复了 INFODATA 命令中对于遍历数据文件时出现空路径的逻辑判断 [#1996](https://github.com/OpenAtomFoundation/pika/pull/1996) @[Mixficsol](https://github.com/Mixficsol)

- 完善了文档的一些部分缺失的描述 [#1962](https://github.com/OpenAtomFoundation/pika/pull/1962) @[baerwang](https://github.com/baerwang)

- 使用 make -j 提高构建速度 [#1933](https://github.com/OpenAtomFoundation/pika/pull/1933) @[xiezheng-XD](https://github.com/xiezheng-XD)

- 解决了 Codis 在线上出现大毛刺的问题 [#2016](https://github.com/OpenAtomFoundation/pika/pull/2016) @[chejinge](https://github.com/chejinge)

- 修复了 Macos 环境下编译使用 tools 导致编译不过的问题 [#2011](https://github.com/OpenAtomFoundation/pika/pull/2011) @[A2ureStone](https://github.com/A2ureStone)
