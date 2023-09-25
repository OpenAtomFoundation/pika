# v3.5.1



## New features

- slow log Adds statistics on queue waiting time [#1997](https://github.com/OpenAtomFoundation/pika/pull/1997) @[wangshao1](https://github.com/wangshao1)

- ReplicationID is used for primary/secondary replication [#1951](https://github.com/OpenAtomFoundation/pika/pull/1951) @[Mixficsol](https://github.com/Mixficsol)

- WAL uses the disablewal command to support dynamic shutdown [#2015](https://github.com/OpenAtomFoundation/pika/pull/2015) @[Mixficsol](https://github.com/Mixficsol)

- The number of threads flushed and the number of threads compaction dynamically adjust into one [#2014](flush线程数和compaction线程数动态调整，合二为一) @[Tianpingan](https://github.com/Tianpingan)

- The RocksDB version was upgraded to v8.3.3 [#1999](https://github.com/OpenAtomFoundation/pika/issues/1999) [dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- Added documentation for configuring Pika in Macos environment [#2003](https://github.com/OpenAtomFoundation/pika/pull/2003) @[klboke](https://github.com/klboke)

- Added the ability to periodically print the length of the work queue to quickly locate problems when the queue is blocked [#1978](https://github.com/OpenAtomFoundation/pika/pull/1978) @[Tianpingan](https://github.com/Tianpingan)

- Added an indicator to detect the entire cluster using a `pika_exporter` [#1953](https://github.com/OpenAtomFoundation/pika/pull/1953) @[chenbt-hz](https://github.com/chenbt-hz)

- Realize automatic registration of Pika service on K8s environment, and automatically register at startup, so as to realize self-organization of cluster [#1931](https://github.com/OpenAtomFoundation/pika/pull/1931)


## bugfix

- Reduces unnecessary log printing by the exporter, reducing CPU utilization [#1945](https://github.com/OpenAtomFoundation/pika/pull/1945) @[Mixficsol](https://github.com/Mixficsol)

- The sentinel mechanism has been upgraded to carry out logical deletion of primary nodes that fall offline [#1949](https://github.com/OpenAtomFoundation/pika/pull/1949) @[Mixficsol](https://github.com/Mixficsol)

- Adjust the rate_limit parameter to fix the situation that RPS is 0 during pressure measurement [#2009](https://github.com/OpenAtomFoundation/pika/pull/2009) @[chejinge](https://github.com/chejinge)

- Fixed the logical determination of empty path when traversing data files in INFODATA command [#1996](https://github.com/OpenAtomFoundation/pika/pull/1996) @[Mixficsol](https://github.com/Mixficsol)

- Improved the description of some missing parts of the document [#1962](https://github.com/OpenAtomFoundation/pika/pull/1962) @[baerwang](https://github.com/baerwang)

- Use make-j to improve build speed [#1933](https://github.com/OpenAtomFoundation/pika/pull/1933) @[xiezheng-XD](https://github.com/xiezheng-XD)

- Fixed an issue where large burrs appeared on the Codis line [#2016](https://github.com/OpenAtomFoundation/pika/pull/2016) @[chejinge](https://github.com/chejinge)

- Fixed an issue where tools could not be compiled in Macos [#2011](https://github.com/OpenAtomFoundation/pika/pull/2011) @[A2ureStone](https://github.com/A2ureStone)



v3.5.0

## 重大特性

### 1.去除 Rsync

* 在 v3.5.0 版本我们去除了 Rsync，使用自研的全量同步方案替代 Rsync，这个版本的全量同步有断点续传，速率限制，文件检验等特点

  [#1805](https://github.com/OpenAtomFoundation/pika/pull/1805)  [#1591](https://github.com/OpenAtomFoundation/pika/issues/1591) [#1816](https://github.com/OpenAtomFoundation/pika/pull/1816) [#1818](https://github.com/OpenAtomFoundation/pika/pull/1818) [#1820](https://github.com/OpenAtomFoundation/pika/pull/1820)

  [@wangshao1](https://github.com/wangshao1) [@chejinge](https://github.com/chejinge) [@luky116](https://github.com/luky116)

1. 去除 Rsync
2. 实现断点续传，速率限制，文件检验
3. Pika 主从同步时，进行 master run_id 的检验

### 2.兼容更多 Redis 命令

* 在 v3.5.0 版本我们兼容并支持了更多的 Redis 命令

  [#1273](https://github.com/OpenAtomFoundation/pika/pull/1273) [#1660](https://github.com/OpenAtomFoundation/pika/pull/1660) [#1245](https://github.com/OpenAtomFoundation/pika/pull/1245) [#1548](https://github.com/OpenAtomFoundation/pika/pull/1548) [#1843](https://github.com/OpenAtomFoundation/pika/pull/1843)

  [@wanghenshui](https://github.com/wanghenshui)  [@Mixficsol](https://github.com/Mixficsol) [@weedge](https://github.com/weedge) [@cheniujh](https://github.com/cheniujh)

1. 支持 UNLINK 命令
2. 支持 INFO COMMANDSTATS 命令
3. 支持 HELLO、SETNAME 命令
4. 支持 BLPOP、BRPOP 命令
5. 新增 Pika 原创命令 DISKRECOVERY

### 3.RocksDB 版本升级和分级压缩

- 在 v3.5.0 版本我们对 RocksDB 进行了升级使其达到了 v8.1.1 版本，实现分级压缩

  [#1396](https://github.com/OpenAtomFoundation/pika/pull/1396) [#1365](https://github.com/OpenAtomFoundation/pika/pull/1365)  [#1189](https://github.com/OpenAtomFoundation/pika/pull/1189)

  [@lqxhub](https://github.com/lqxhub)  [@chejinge](https://github.com/chejinge)

1. 升级 RocksDB 版本到 v8.1.1
2. 实现 RocksDB 分级压缩
3. 新增 RocksDB 缓存配置项 num-shard-bits 能够从配置文件中读取

### 4.支持 Blobdb

* 在 v3.5.0 版本我们支持 BlobDB KV 分离

  [#1456](https://github.com/OpenAtomFoundation/pika/pull/1456)

  [@lqxhub](https://github.com/lqxhub)

1. 支持 BlobDB KV 分离

### 5.基于 Codis 的集群模式

* 在 v3.5.0 版本中我们引入了 Codis 集群模式并支持了 Codis 迁移 slot 的命令

  [#1279](https://github.com/OpenAtomFoundation/pika/pull/1279) [#1311](https://github.com/OpenAtomFoundation/pika/pull/1311) [#1492](https://github.com/OpenAtomFoundation/pika/pull/1492) [#1662](https://github.com/OpenAtomFoundation/pika/pull/1662) [#1682](https://github.com/OpenAtomFoundation/pika/pull/1682) [#1632](https://github.com/OpenAtomFoundation/pika/pull/1632)  [#1700](https://github.com/OpenAtomFoundation/pika/pull/1700)

  [@WyattJia](https://github.com/WyattJia) [@chejinge](https://github.com/chejinge) [@luky116](https://github.com/luky116) [@yaoyinnan](https://github.com/yaoyinnan)

1. 引入 Codis 到 Pika
2. 引入 Codis 的 CI
3. 支持 Codis 迁移 slot 命令
4. 新增是否在 reload 的 slotmigrate 状态

### 6.可观测性

- 在 v3.5.0 版本我们新加了 pika_exporter 工具来监测 Pika 的可观测性指标，并在后期添加了一些新的指标来完善 Pika 的可观测性

  [#1388](https://github.com/OpenAtomFoundation/pika/pull/1388)  [#1733](https://github.com/OpenAtomFoundation/pika/pull/1733)  [#1751](https://github.com/OpenAtomFoundation/pika/pull/1751)  [#1736](https://github.com/OpenAtomFoundation/pika/pull/1736) [#1560](https://github.com/OpenAtomFoundation/pika/pull/1560)  [#1696](https://github.com/OpenAtomFoundation/pika/pull/1696)

  [@chenbt-hz](https://github.com/chenbt-hz)  [@yaoyinnan](https://github.com/yaoyinnan) [@Mixficsol](https://github.com/Mixficsol)

1. 新增 Pika 可观测系统 pika_exporter
2. 新增网络 I/O 流量监控指标
3. 新增命令统计耗时指标
4. 新增 estimate_pending_compaction_bytes 度量来分析碎片率指标
5. 新增 RocksDB 指标

### 7.云原生部署

- 在 v3.5.0 版本我们实现了一个 pika-operator mvp 版本，主要实现了 Pika 单实例服务在 K8s 上的快速部署

  [#1243](https://github.com/OpenAtomFoundation/pika/pull/1243) [#1330](https://github.com/OpenAtomFoundation/pika/pull/1330) [#1347](https://github.com/OpenAtomFoundation/pika/pull/1347)

  [@machinly](https://github.com/machinly)

1. 实现 Pika 单例服务在 K8s 上快速部署
2. 实现了在 MiniKube 环境中部署 Pika
3. 给 pika-operator 添加 E2E 测试

### 8.跨平台编译

* 在 v3.5.0 版本，Pika 支持 MacOS 平台，CentOS 平台，Ubuntu 平台进行编译使用

  [#1372](https://github.com/OpenAtomFoundation/pika/pull/1372)  [#1488](https://github.com/OpenAtomFoundation/pika/pull/1488)

  [@loveyacper](https://github.com/loveyacper)  [@Mixficsol](https://github.com/Mixficsol)

1. 支持 MacOS 平台

### 9.多平台 CI、Go 集成测试、TCL 单元测试、PythonE2E 测试、CTest 单元测试

* 在 v3.5.0 版本我们支持了 Ubuntu，CentOS，MacOS 下的 CI，在测试方面新加了 Go 的集成测试，TCL 单元测试，Python 的 E2E 测试

  [#1534](https://github.com/OpenAtomFoundation/pika/pull/1534) [#1772](https://github.com/OpenAtomFoundation/pika/pull/1772)  [#1769](https://github.com/OpenAtomFoundation/pika/pull/1769)  [#1347](https://github.com/OpenAtomFoundation/pika/pull/1347)  [#1268](https://github.com/OpenAtomFoundation/pika/pull/1268)  [#1693](https://github.com/OpenAtomFoundation/pika/pull/1693) [#1362](https://github.com/OpenAtomFoundation/pika/pull/1362)  [#1538](https://github.com/OpenAtomFoundation/pika/pull/1538)  [#1246](https://github.com/OpenAtomFoundation/pika/pull/1246)   [#1357](https://github.com/OpenAtomFoundation/pika/pull/1357)  [#1534](https://github.com/OpenAtomFoundation/pika/pull/1534) [#1772](https://github.com/OpenAtomFoundation/pika/pull/1772)

  [@machinly](https://github.com/machinly)  [@wanghenshui](https://github.com/wanghenshui) [@chenbt-hz](https://github.com/chenbt-hz)  [@Tangruilin](https://github.com/Tangruilin)  [@kernelai](https://github.com/kernelai) [@Mixficsol](https://github.com/Mixficsol)

1. 新增 CentOS 环境下的 CI
2. 新增 MacOS 环境下的 CI
3. 新增 E2E 测试框架
4. 新增在 Github CI Workflow 中添加 CMake 编译环境
5. 新增在 TCL 脚本中 populate 方法模拟 Redis debug populate 方法，用以填充测试数据
6. 新增单测启动脚本
7. 新增在 blackwidow 中添加 cmake 文件，添加对 blackwidow 的单元测试
8. 使用 CTest 进行单元测试
9. 移植 Redis 测试脚本
10. 在 CI 流程中添加新的任务，以验证 Pika 是否能在 CentOS 系统上成功编译

# 新增功能

- 新增对 compact 进行自动限速，降低 compact 对上层数据读写延迟造成影响 [#1374](https://github.com/OpenAtomFoundation/pika/pull/1374) [@wanghenshui](https://github.com/wanghenshui)
- 新增 aof_to_pika 工具集 [#1340](https://github.com/OpenAtomFoundation/pika/pull/1340) [@Axlgrep](https://github.com/Axlgrep)
- 新增 Pika 的打印 Logo [#1787](https://github.com/OpenAtomFoundation/pika/pull/1787) [@Mixficsol](https://github.com/Mixficsol)
- 新增使用 clang-tidy 优化了部分代码 [#1701](https://github.com/OpenAtomFoundation/pika/pull/1701)  [#1730](https://github.com/OpenAtomFoundation/pika/pull/1730)  [@longfar-ncy](https://github.com/longfar-ncy)
- 新增将 C++98 风格的代码统一更换成 C++11 风格，使用 C++ 编码规范编程 [#1684](https://github.com/OpenAtomFoundation/pika/pull/1684) [@chejinge](https://github.com/chejinge)
- 新增 snappy 库 [#1216](https://github.com/OpenAtomFoundation/pika/pull/1216) [@kernelai](https://github.com/kernelai)
- 新增 CLA 文件 [#1260](https://github.com/OpenAtomFoundation/pika/pull/1260) [@kernelai](https://github.com/kernelai)
- 新增内存相关的配置支持 K|M|G 单位 [#1307](https://github.com/OpenAtomFoundation/pika/pull/1307) [@lqxhub](https://github.com/lqxhub)
- 新增添加 libunwind 依赖库 [#1316](https://github.com/OpenAtomFoundation/pika/pull/1316) [@kernelai](https://github.com/kernelai)
- 新增编译 Pika 脚本 [#1356](https://github.com/OpenAtomFoundation/pika/pull/1356) [@lqxhub](https://github.com/lqxhub)
- 新增 pika.conf 中添加 "rate-limiter-bandwidth" 选项 [#1272](https://github.com/OpenAtomFoundation/pika/pull/1272) [@wanghenshui](https://github.com/wanghenshui)
- 新增 INFO 命令添加 redis_version 信息 [#1403](https://github.com/OpenAtomFoundation/pika/pull/1403) [@wanghenshui](https://github.com/wanghenshui)
- 新增对 aof_to_pika 的 CMake 支持 [#1436](https://github.com/OpenAtomFoundation/pika/pull/1436) [@A2ureStone](https://github.com/A2ureStone)
- 新增 clang 格式的 shell 脚本 [#1448](https://github.com/OpenAtomFoundation/pika/pull/1448) [@lqxhub](https://github.com/lqxhub)
- 新增配置项禁用编译 command docs [#1642](https://github.com/OpenAtomFoundation/pika/pull/1642) [@tedli](https://github.com/tedli)
- 新增 benchmark_client， binlog_sender，manifest_generator，rdb_to_pika，txt_to_pika，pika_to_txt，pika_port 的 CMake 支持 [#1451](https://github.com/OpenAtomFoundation/pika/pull/1451) [@A2ureStone](https://github.com/A2ureStone)

# 改进

- Pika 模式统一去除 sharding 模式 [#1481](https://github.com/OpenAtomFoundation/pika/pull/1481) [@chejinge](https://github.com/chejinge)
- 在 config 中添加槽位号 [#1664](https://github.com/OpenAtomFoundation/pika/pull/1664) [@luky116](https://github.com/luky116)
- 统一 partition 命名为 slot [#1571](https://github.com/OpenAtomFoundation/pika/pull/1571)  [#1655](https://github.com/OpenAtomFoundation/pika/pull/1655) [@chejinge](https://github.com/chejinge)
- 用 Monitor 命令替换 Monitor 线程 [#1551](https://github.com/OpenAtomFoundation/pika/pull/1551) [@loveyacper](https://github.com/loveyacper)
- 增加了启动服务器时对空密码和错误密码的 requipass 判断 [#1645](https://github.com/OpenAtomFoundation/pika/pull/1645) [@hqh-cell](https://github.com/hqh-cell)
- 重构 pstd_mutex [#1440](https://github.com/OpenAtomFoundation/pika/pull/1440) [@4kangjc](https://github.com/4kangjc)
- 修改命令信息键空间响应数据 [#1450](https://github.com/OpenAtomFoundation/pika/pull/1450) [@lqxhub](https://github.com/lqxhub)
- 合并 pstd 和 storage 的相同代码 [#1419](https://github.com/OpenAtomFoundation/pika/pull/1419) [@4kangjc](https://github.com/4kangjc)
- 按计数添加 spop [#1434](https://github.com/OpenAtomFoundation/pika/pull/1434) [@ChanphongGu](https://github.com/ChanphongGu)
- 添加 Pika docker 镜像说明 [#1557](https://github.com/OpenAtomFoundation/pika/pull/1557) [@machinly](https://github.com/machinly)
- 用 RocksDB GetFileSystem 接口替换旧有的 GetEnv 接口 [#1213](https://github.com/OpenAtomFoundation/pika/pull/1213) [@kernelai](https://github.com/kernelai)
- 新增散落在外的 Pika 文档 [#1193](https://github.com/OpenAtomFoundation/pika/pull/1193) [@fengpeiyuan](https://github.com/fengpeiyuan)
- CMake 升级到 3.18 [#1366](https://github.com/OpenAtomFoundation/pika/pull/1366) [@AlexStocks](https://github.com/AlexStocks)
- 给 Pika 添加 format 工具 [#1512](https://github.com/OpenAtomFoundation/pika/pull/1512) [@liuqian1990](https://github.com/liuqian1990)
- 为 Pika 添加 clang-tidy [#1515](https://github.com/OpenAtomFoundation/pika/pull/1515)  [#1730](https://github.com/OpenAtomFoundation/pika/pull/1730) [@ForestLH](https://github.com/ForestLH) [@longfar-ncy](https://github.com/longfar-ncy)
- 在 MiniKube 中把 Pika 和 pika-operator 运行起来 [#1330](https://github.com/OpenAtomFoundation/pika/pull/1330) [@machinly](https://github.com/machinly)
- 给 clang-format 添加谷歌编码风格 [#1479](https://github.com/OpenAtomFoundation/pika/pull/1479) [@Mixficsol](https://github.com/Mixficsol)
- 使用 std::filesystem 重构 pstd / env [#1470](https://github.com/OpenAtomFoundation/pika/pull/1470) [@4kangjc](https://github.com/4kangjc)
- pstd::GetFixed 添加 Slice [#1425](https://github.com/OpenAtomFoundation/pika/pull/1425) [@4kangjc](https://github.com/4kangjc)
- 在头文件中使用内联变量 [#1407](https://github.com/OpenAtomFoundation/pika/pull/1407) [@4kangjc](https://github.com/4kangjc)
- 使用 clang-format 对代码进行格式化 [#1378](https://github.com/OpenAtomFoundation/pika/pull/1378) [@AlexStocks](https://github.com/AlexStocks)
- 移动 pika-operator 到 pika-tools 目录 [#1468](https://github.com/OpenAtomFoundation/pika/pull/1468) [@luky116](https://github.com/luky116)
- 给 Pika 添加 Codis -> Pika 迁移工具 [#1514](https://github.com/OpenAtomFoundation/pika/pull/1514) [@chenbt-hz](https://github.com/chenbt-hz)

# Bug 修复

- 修复不同 CPU 指令集不兼容导致 coredump [#1812](https://github.com/OpenAtomFoundation/pika/pull/1812) [@chejinge](https://github.com/chejinge)
- 修复 Monitor 命令异常退出问题 [#1804](https://github.com/OpenAtomFoundation/pika/pull/1804) [@Mixficsol](https://github.com/Mixficsol)
- 修复不完整的配置覆盖 [#1800](https://github.com/OpenAtomFoundation/pika/pull/1800) [@yaoyinnan](https://github.com/yaoyinnan)
- 修复 MacOS 环境下的 CTest 测试 [#1789](https://github.com/OpenAtomFoundation/pika/pull/1789)  [#1721](https://github.com/OpenAtomFoundation/pika/pull/1721) [#1782](https://github.com/OpenAtomFoundation/pika/pull/1782)  [@tedli](https://github.com/tedli) [@Mixficsol](https://github.com/Mixficsol)
- 修复进程启动时从配置文件加载 master-run-id 失败，导致从节点需要重新同步历史数据 [#1778](https://github.com/OpenAtomFoundation/pika/pull/1778) [@luky116](https://github.com/luky116)
- 修复 Pika 单测时 overrides 选项格式出错 [#1768](https://github.com/OpenAtomFoundation/pika/pull/1768) [@A2ureStone](https://github.com/A2ureStone)
- 修复通过配置指定获取 INFO xxx (INFO data, INFO stats) 的索引，而不是使用INFO ALL [#1762](https://github.com/OpenAtomFoundation/pika/pull/1762) [@yaoyinnan](https://github.com/yaoyinnan)
- 修复 instantaneous_metric 并每5秒收集一次网络指标 [#1757](https://github.com/OpenAtomFoundation/pika/pull/1757) [@yaoyinnan](https://github.com/yaoyinnan)
- 修复完善 config get 返回的参数 [#1593](https://github.com/OpenAtomFoundation/pika/issues/1593) [@luky116](https://github.com/luky116)
- 修复 slot 可能为空的问题 [#1712](https://github.com/OpenAtomFoundation/pika/pull/1712) [@Mixficsol](https://github.com/Mixficsol)
- 修复中 makefile 中对版本选择执行的一些 bug [#1714](https://github.com/OpenAtomFoundation/pika/pull/1714) [@luky116](https://github.com/luky116)
- 修复 HSET 第一次不能成功执行的问题 [#1710](https://github.com/OpenAtomFoundation/pika/pull/1710) [@Mixficsol](https://github.com/Mixficsol)
- 修复 slave 节点的 binlog offset 比 master 节点大，导致 TrySync 失败 [#1681](https://github.com/OpenAtomFoundation/pika/issues/1681) [@luky116](https://github.com/luky116)
- 修复了一个 NOAUTH 错误的测试，该错误是任意命令，当需要 AUTH 时给出错误 [#1680](https://github.com/OpenAtomFoundation/pika/pull/1680) [@hqh-cell](https://github.com/hqh-cell)
- 修复了 binlog 文件超出上限不会自动清理问题 [#1679](https://github.com/OpenAtomFoundation/pika/pull/1679) [@luky116](https://github.com/luky116)
- 修复 LPUSHX 和 RPUSHX 多元素插入的问题 [#1525](https://github.com/OpenAtomFoundation/pika/pull/1525) [@ForestLH](https://github.com/ForestLH)
- 修复 BGSAVE 命令执行报错问题 [#1523](https://github.com/OpenAtomFoundation/pika/pull/1523) [@Brokenice0415](https://github.com/Brokenice0415)
- 修复关闭 RocksDB 使用 io_uring， 防止在链接时报错 [#1489](https://github.com/OpenAtomFoundation/pika/pull/1489) [@lqxhub](https://github.com/lqxhub)
- 修复 MacOS 环境下 Pika 编译警告问题 [#1740](https://github.com/OpenAtomFoundation/pika/pull/1740) [@Mixficsol](https://github.com/Mixficsol)
- 修复不支持的命令报错消息，适配 go-redis [#1244](https://github.com/OpenAtomFoundation/pika/pull/1244) [@wgqi1126](https://github.com/wgqi1126)
- 修复一个 protobuf3 的编译告警 [#1267](https://github.com/OpenAtomFoundation/pika/pull/1267) [@wanghenshui](https://github.com/wanghenshui)
- 修复 sharding 模式下 EXISTS 和 DEL 两个命令的 bug [#1277](https://github.com/OpenAtomFoundation/pika/pull/1277) [@wanghenshui](https://github.com/wanghenshui)
- 修复 rsync auth bug [#1278](https://github.com/OpenAtomFoundation/pika/pull/1278) [@wanghenshui](https://github.com/wanghenshui)
- 修复按照 Redis 标准修复 rewrite 返回结果形式 [#1339](https://github.com/OpenAtomFoundation/pika/pull/1339) [@lqxhub](https://github.com/lqxhub)
- 修复修改 SET NX/XX 返回值，与 Redis 保持一致 [#1343](https://github.com/OpenAtomFoundation/pika/pull/1343) [@gtygo](https://github.com/gtygo)
- 修复 exporter 中缺失 pika_master_link_status 和 pika_slave_priority indicators 两张图 [#1726](https://github.com/OpenAtomFoundation/pika/pull/1726) [@Mixficsol](https://github.com/Mixficsol)
- 修复防止当 Pika 使用的文件描述符超过 ulimit 限制时程序 coredump [#1346](https://github.com/OpenAtomFoundation/pika/pull/1346) [@kernelai](https://github.com/kernelai)
- 修复在类中初始化群成员的值以防止 redis-cli 与 Pika 的链接被意外关闭 [#1390](https://github.com/OpenAtomFoundation/pika/pull/1390) [@AlexStocks](https://github.com/AlexStocks)
- 修复 delete_dir snprintf bug [#1400](https://github.com/OpenAtomFoundation/pika/pull/1400) [@wanghenshui](https://github.com/wanghenshui)
- 修复限制 thread num [#1401](https://github.com/OpenAtomFoundation/pika/pull/1401) [@wanghenshui](https://github.com/wanghenshui)
- 修复 redis-benchmark 从 Pika 获取 config [#1402](https://github.com/OpenAtomFoundation/pika/pull/1402) [@wanghenshui](https://github.com/wanghenshui)
- 修复初始化类成员 [#1406](https://github.com/OpenAtomFoundation/pika/pull/1406) [@AlexStocks](https://github.com/AlexStocks)
- 修复 MacOS 环境下主从同步测试的问题 [#1776](https://github.com/OpenAtomFoundation/pika/pull/1776) [@Mixficsol](https://github.com/Mixficsol)
- 修复 NULL 为 nullptr [#1408](https://github.com/OpenAtomFoundation/pika/pull/1408) [@AlexStocks](https://github.com/AlexStocks)
- 修复 ttl 的 size 边界设置过期时间戳 [#1413](https://github.com/OpenAtomFoundation/pika/pull/1413) [@ForestLH](https://github.com/ForestLH)
- 修复访问空指针 [#1414](https://github.com/OpenAtomFoundation/pika/pull/1414) [#1426](https://github.com/OpenAtomFoundation/pika/pull/1426) [@fly1ngpengu1ns](https://github.com/fly1ngpengu1ns) [@Pikachu1412](https://github.com/Pikachu1412)
- 修复 Monitor 命令 [#1424](https://github.com/OpenAtomFoundation/pika/pull/1424) [@loveyacper](https://github.com/loveyacper)
- 修复 partitionInfo::operator 回布尔值 [#1427](https://github.com/OpenAtomFoundation/pika/pull/1427) [@4kangjc](https://github.com/4kangjc)
- 修复访问 src/net 中的空指针问题 [#1453](https://github.com/OpenAtomFoundation/pika/pull/1453) [@gitveg](https://github.com/gitveg)
- 修复 aof_reader c++17 构建失败 [#1454](https://github.com/OpenAtomFoundation/pika/pull/1454) [@lqxhub](https://github.com/lqxhub)
- 修复编译警告 [#1455](https://github.com/OpenAtomFoundation/pika/pull/1455) [@wangchuande](https://github.com/wangchuande)
- 修复 operator 构建偶尔失败的问题 [#1464](https://github.com/OpenAtomFoundation/pika/pull/1464) [@machinly](https://github.com/machinly)
- 修复 CondVar 无法退出的问题 [#1475](https://github.com/OpenAtomFoundation/pika/pull/1475) [@lqxhub](https://github.com/lqxhub)
- 修复 libfmt 调试构建名称错误 [#1476](https://github.com/OpenAtomFoundation/pika/pull/1476) [@lqxhub](https://github.com/lqxhub)
- 修复 Monitor 命令异常退出问题 [#1804](https://github.com/OpenAtomFoundation/pika/pull/1804) [@Mixficsol](https://github.com/Mixficsol)
- 修复 slave 节点 binlog offset 异常问题 [#1681](https://github.com/OpenAtomFoundation/pika/issues/1681) [@luky116](https://github.com/luky116)
- 修复单元测试 no auth 报错问题 [#1680](https://github.com/OpenAtomFoundation/pika/pull/1680) [@hqh-cell](https://github.com/hqh-cell)
- 修复 binlog 文件超过上限不会被删除的问题 [#1679](https://github.com/OpenAtomFoundation/pika/pull/1679) [@luky116](https://github.com/luky116)
- 修复 string 相关单元测试 [#1675](https://github.com/OpenAtomFoundation/pika/pull/1675) [@Mixficsol](https://github.com/Mixficsol)
- 修复 Codis 异常退出时 tmp 文件没有删除的问题 [#1661](https://github.com/OpenAtomFoundation/pika/issues/1661) [@yaoyinnan](https://github.com/yaoyinnan)
- 修复主从复制数据不一致的问题 [#1638](https://github.com/OpenAtomFoundation/pika/issues/1638) [@cheniujh](https://github.com/cheniujh)
- 修复 INFO 命令 Metrics 指标获取失败的问题 [#1646](https://github.com/OpenAtomFoundation/pika/issues/1646) [@chejinge](https://github.com/chejinge)
- 修复单侧 basic 类型错误的问题 [#1625](https://github.com/OpenAtomFoundation/pika/pull/1625) [@chenbt-hz](https://github.com/chenbt-hz)
- 修复 LPOP, RPOP 接口提示参数不对的问题 [#1520](https://github.com/OpenAtomFoundation/pika/issues/1520) [@luky116](https://github.com/luky116)
- 修复实例硬盘容量满清理数据后，Pika 仍然保持错误的状态，需要重启才能恢复的问题 [#1765](https://github.com/OpenAtomFoundation/pika/pull/1765) [#1822](https://github.com/OpenAtomFoundation/pika/pull/1822) [#1843](https://github.com/OpenAtomFoundation/pika/pull/1843) [@Yangsx-1](https://github.com/Yangsx-1) [@Mixficsol](https://github.com/Mixficsol)

# 注意事项

* 删除 3.4 引入的 Proxy [#1274](https://github.com/OpenAtomFoundation/pika/pull/1274) [@wanghenshui](https://github.com/wanghenshui)
* 删除 submodule [#1312](https://github.com/OpenAtomFoundation/pika/pull/1312) [@wanghenshui](https://github.com/wanghenshui)
* 删除 sharding 命令 [#1469](https://github.com/OpenAtomFoundation/pika/pull/1469) [@chejinge](https://github.com/chejinge)
* 删除 tcmalloc [#1472](https://github.com/OpenAtomFoundation/pika/pull/1472) [@Mixficsol](https://github.com/Mixficsol)
* 对 leader 添加 protect 模式 [#1276](https://github.com/OpenAtomFoundation/pika/pull/1276) [@wanghenshui](https://github.com/wanghenshui)
* 把 Codis 最新代码同步到 Pika 仓库中 [#1279](https://github.com/OpenAtomFoundation/pika/pull/1279) [@WyattJia](https://github.com/WyattJia)
* 在 CMake 文件中添加外部依赖库 [#1281](https://github.com/OpenAtomFoundation/pika/pull/1281) [@kernelai](https://github.com/kernelai)
* 在 CMake 文件中添加外部依赖库列表 [#1285](https://github.com/OpenAtomFoundation/pika/pull/1285) [@kernelai](https://github.com/kernelai)
* 将 Pika 制作了一个静态链接库，可以直接和依赖库链接构成 Pika 的二进制包 [#1310](https://github.com/OpenAtomFoundation/pika/pull/1310) [@Tangruilin](https://github.com/Tangruilin)
* protbuf 生成不依赖本机环境，加多核编译，修复 debian 下连接缺少 pthread 的报错 [#1314](https://github.com/OpenAtomFoundation/pika/pull/1314) [@lqxhub](https://github.com/lqxhub)
* 把 slash 重命名为 pstd [#1348](https://github.com/OpenAtomFoundation/pika/pull/1348) [@AlexStocks](https://github.com/AlexStocks)
* 把目录 src/pink 重命名为 src/net [#1350](https://github.com/OpenAtomFoundation/pika/pull/1350) [@AlexStocks](https://github.com/AlexStocks)
* 把单词 bw 和 blackwidow 重命名为 storage [#1352](https://github.com/OpenAtomFoundation/pika/pull/1352)  [#1353](https://github.com/OpenAtomFoundation/pika/pull/1353) [@AlexStocks](https://github.com/AlexStocks)
* 更新编译器要求文档 [#1420](https://github.com/OpenAtomFoundation/pika/pull/1420) [@4kangjc](https://github.com/4kangjc)
* 替换无效链接 [#1430](https://github.com/OpenAtomFoundation/pika/pull/1430) [#1441](https://github.com/OpenAtomFoundation/pika/pull/1441) [#1471](https://github.com/OpenAtomFoundation/pika/pull/1471) [@gitveg](https://github.com/gitveg) [@1321034767](https://github.com/1321034767) [@chiehwarm](https://github.com/chiehwarm)
* 将 /tools/codis2pika/test 中的 Redis 从 4.3.3 升级到 4.4.4 [#1536](https://github.com/OpenAtomFoundation/pika/pull/1536) [@dependabot]()
* 将 golang.org/x/sys 从 /tools/codis2pika 中的 0.0.0-20210927094055-39ccf1dd6fa6 提升到 0.1.0 [#1535](https://github.com/OpenAtomFoundation/pika/pull/1535) [@dependabot]()
* 用智能指针替换 new/delete [#1503](https://github.com/OpenAtomFoundation/pika/pull/1503)  [#1502](https://github.com/OpenAtomFoundation/pika/pull/1502) [#1493](https://github.com/OpenAtomFoundation/pika/pull/1493) [@cheniujh](https://github.com/cheniujh) [@A2ureStone](https://github.com/A2ureStone) [@iiiuwioajdks](https://github.com/iiiuwioajdks)
* 用 glog 替换 fprintf [#1421](https://github.com/OpenAtomFoundation/pika/pull/1421) [@Mixficsol](https://github.com/Mixficsol)



# v3.5.1



## 新特性

- slow log 增加队列等待时间统计 [#1997](https://github.com/OpenAtomFoundation/pika/pull/1997) @[wangshao1](https://github.com/wangshao1)

- 主从复制使用 ReplicationID [#1951](https://github.com/OpenAtomFoundation/pika/pull/1951) @[Mixficsol](https://github.com/Mixficsol)

- WAL 以 disablewal 命令方式支持动态关闭 [#2015](https://github.com/OpenAtomFoundation/pika/pull/2015) @[Mixficsol](https://github.com/Mixficsol)

- flush 线程数和 compaction 线程数动态调整，合二为一 [#2014](flush线程数和compaction线程数动态调整，合二为一) @[Tianpingan](https://github.com/Tianpingan)

- 升级了 RocksDB 版本到 v8.3.3 [#1999](https://github.com/OpenAtomFoundation/pika/issues/1999) [dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- 增加了 Macos 环境下配置 Pika 的使用方法文档 [#2003](https://github.com/OpenAtomFoundation/pika/pull/2003) @[klboke](https://github.com/klboke)

- 新增周期性打印工作队列的长度功能，在队列阻塞的时候可以快速定位问题 [#1978](https://github.com/OpenAtomFoundation/pika/pull/1978) @[Tianpingan](https://github.com/Tianpingan)

- 新增利用一个 `pika_exporter` 检测整个集群的指标 [#1953](https://github.com/OpenAtomFoundation/pika/pull/1953) @[chenbt-hz](https://github.com/chenbt-hz)

- 实现在 K8s 环境上 Pika 服务的自动注册，在启动时自动注册，从而实现集群的自组织 [#1931](https://github.com/OpenAtomFoundation/pika/pull/1931)

  ​

## bugfix

- 减少了 exporter 非必要的日志打印，降低 CPU 利用率 [#1945](https://github.com/OpenAtomFoundation/pika/pull/1945) @[Mixficsol](https://github.com/Mixficsol)

- 升级了 sentinel 机制，对于掉线的主节点，进行逻辑删除 [#1949](https://github.com/OpenAtomFoundation/pika/pull/1949) @[Mixficsol](https://github.com/Mixficsol)

- 调整 rate_limit 参数，修复压测时出现 RPS 为 0 的情况 [#2009](https://github.com/OpenAtomFoundation/pika/pull/2009) @[chejinge](https://github.com/chejinge)

- 修复了 INFODATA 命令中对于遍历数据文件时出现空路径的逻辑判断 [#1996](https://github.com/OpenAtomFoundation/pika/pull/1996) @[Mixficsol](https://github.com/Mixficsol)

- 完善了文档的一些部分缺失的描述 [#1962](https://github.com/OpenAtomFoundation/pika/pull/1962) @[baerwang](https://github.com/baerwang)

- 使用 make -j 提高构建速度 [#1933](https://github.com/OpenAtomFoundation/pika/pull/1933) @[xiezheng-XD](https://github.com/xiezheng-XD)

- 解决了 Codis 在线上出现大毛刺的问题 [#2016](https://github.com/OpenAtomFoundation/pika/pull/2016) @[chejinge](https://github.com/chejinge)

- 修复了 Macos 环境下编译使用 tools 导致编译不过的问题 [#2011](https://github.com/OpenAtomFoundation/pika/pull/2011) @[A2ureStone](https://github.com/A2ureStone)

  ​

  ​