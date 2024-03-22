# v3.5.3

## New features

- Pika supports ACL[#2013](https://github.com/OpenAtomFoundation/pika/pull/2013) @[lqxhub](https://github.com/lqxhub)

- Automatically resume service when Codis dashboard coroutine panics[#2349](https://github.com/OpenAtomFoundation/pika/pull/2349)@[chengyu-l](https://github.com/chengyu-l)

- During the full replication process, the slave node of the pika service does not receive read traffic requests.[#2197](https://github.com/OpenAtomFoundation/pika/pull/2197)  @[tedli](https://github.com/tedli)

- Pika cache adds bimap data type.[#2197](https://github.com/OpenAtomFoundation/pika/pull/2253)  @[chejinge](https://github.com/chejinge)

- Delete the remaining Slots in Sharing mode. There is only DB under Pika, and there are multiple DBs under one Pika.[#2251](https://github.com/OpenAtomFoundation/pika/pull/2251) @[Mixficsol](https://github.com/Mixficsol)

- Pika exporter exposes cache-related data collection indicators.[#2318](https://github.com/OpenAtomFoundation/pika/pull/2318) @[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- Pika supports separation of fast and slow commands.[#2162](https://github.com/OpenAtomFoundation/pika/pull/2162) @[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- After pika executes bgsave, retain the unix timepoint.[#2167](https://github.com/OpenAtomFoundation/pika/pull/2167) @[hero-heng](https://github.com/hero-heng)

- Pika supports dynamic configuration of the disable_auto_compations parameter.[#2257](https://github.com/OpenAtomFoundation/pika/pull/2257) @[hero-heng](https://github.com/hero-heng)

- Pika supports Redis Stream.[#1955](https://github.com/OpenAtomFoundation/pika/pull/1955) @[KKorpse](https://github.com/KKorpse)

- Pika supports large key analysis tools[#2195](https://github.com/OpenAtomFoundation/pika/pull/2195) @[sjcsjc123](https://github.com/sjcsjc123)

- Pika supports dynamic adjustment of Pika cache parameters[#2197](https://github.com/OpenAtomFoundation/pika/pull/2197)  @[chejinge](https://github.com/chejinge)

- Updated Pika benchmark tool to support more interface stress tests.[#2222](https://github.com/OpenAtomFoundation/pika/pull/2222)@[wangshao1](https://github.com/wangshao1)

- Pika Operator supports automatic expansion of pika clusters.[#2121](https://github.com/OpenAtomFoundation/pika/pull/2121)@[machinly](https://github.com/machinly/)

- Add the CompactRange command to support compacting keys within a certain range.[#2163](https://github.com/OpenAtomFoundation/pika/pull/2163)@[u6th9d](https://github.com/u6th9d)

- Add small time cost compaction policy.[#2172](https://github.com/OpenAtomFoundation/pika/pull/2172)@[u6th9d](https://github.com/u6th9d)

- Upgrade RocksDB version to v8.7.3.[#2157](https://github.com/OpenAtomFoundation/pika/pull/2157)@[JasirVoriya](https://github.com/JasirVoriya)

- Pika distributed cluster Codis proxy adds new observable indicators.[#2199](https://github.com/OpenAtomFoundation/pika/pull/2199)@[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- Pika distributed cluster supports automatic failover.[#2386](https://github.com/OpenAtomFoundation/pika/pull/2386)@[chengyu-l](https://github.com/chengyu-l)

- Pika supports redis rename-command function.[#2455](https://github.com/OpenAtomFoundation/pika/pull/2455)@[Mixficsol](https://github.com/Mixficsol)

- Optimize codis slot migration speed and support dynamic modification of migration thread and speed.[#2486](https://github.com/OpenAtomFoundation/pika/pull/2486) @[chejinge](https://github.com/chejinge)

- Pika supports dynamic adjustment of the max-conn-rbuf-size parameter.[#2434](https://github.com/OpenAtomFoundation/pika/pull/2434) @[HappyUncle](https://github.com/HappyUncle)

- Pika-operator supports namespace and can deploy different clusters under different namespaces.[#2480](https://github.com/OpenAtomFoundation/pika/pull/2480) @[Y-Rookie](https://github.com/Y-Rookie)

- Pika-operator supports monitoring indicator collection and automatically launches Pika-experter.[#2451](https://github.com/OpenAtomFoundation/pika/pull/2451) @[chengyu-l](https://github.com/chengyu-l)

- ACL forward compatible with userblacklist.[#2459](https://github.com/OpenAtomFoundation/pika/pull/2459) @[dingxiaoshuai](https://github.com/dingxiaoshuai)

- Enriched Pika TCL test set .[#2497](https://github.com/OpenAtomFoundation/pika/pull/2497) @[Mixficsol](https://github.com/Mixficsol)

- Enriched Pika Gotest test set.[#2502](https://github.com/OpenAtomFoundation/pika/pull/2502) @[Mixficsol](https://github.com/Mixficsol)

## bugfix

- Fixed an issue where Pika would accidentally delete dump files during full replication from the node.[#2377](https://github.com/OpenAtomFoundation/pika/pull/2377)@[wangshao1](https://github.com/wangshao1)

- Fixed the processing logic after the slave node receives an abnormal response packet from the master during the master-slave replication process.[#2319](https://github.com/OpenAtomFoundation/pika/pull/2319)@[wangshao1](https://github.com/wangshao1)

- Call disable compaction when pika executes the shutdown command to improve the process exit speed. [#2345](https://github.com/OpenAtomFoundation/pika/pull/2345) @[panlei-coder](https://github.com/panlei-coder)

- Fix the problem of inaccurate Codis-dashboard Redis Memory value.[#2337](https://github.com/OpenAtomFoundation/pika/pull/2337) @[Mixficsol](https://github.com/Mixficsol)

- The INFO command is time-consuming and optimized to reduce the frequency of disk checks. [#2197](https://github.com/OpenAtomFoundation/pika/pull/2197)  @[chejinge](https://github.com/chejinge)

- Fixed the issue where rsync deletes temporary files with incorrect paths and fails to delete them, causing rocksdb to fail to open.[#2186](https://github.com/OpenAtomFoundation/pika/pull/2186)@[wangshao1](https://github.com/wangshao1)

- Fixed the problem that the compact, bgsave, and info keyspace commands did not specify the db name, resulting in some coredump commands.[#2194](https://github.com/OpenAtomFoundation/pika/pull/2194)@[u6th9d](https://github.com/u6th9d)

- Codis dashboard uses info replication instead of info command to search master ip to reduce the performance impact on Pika. [#2198](https://github.com/OpenAtomFoundation/pika/pull/2198)  @[chenbt-hz](https://github.com/chenbt-hz)

- Fix Pika cache to use edge cases to solve the problem of cache and DB data inconsistency in some scenarios.[#2225](https://github.com/OpenAtomFoundation/pika/pull/2225)  @[chejinge](https://github.com/chejinge)

- Fixed the issue where Segmentation fault would be reported when the dump folder is empty.[#2265](https://github.com/OpenAtomFoundation/pika/pull/2265)  @[chenbt-hz](https://github.com/chenbt-hz)

- Fixed the problem that some command caches did not take effect due to flag calculation errors.[#2217](https://github.com/OpenAtomFoundation/pika/pull/2217) @[lqxhub](https://github.com/lqxhub)

- Fixed the problem that in master-slave replication mode, after the master instance flushdb, the slave instance cannot be accessed due to deadlock.[#2249](https://github.com/OpenAtomFoundation/pika/pull/2249)@[ForestLH](https://github.com/ForestLH)

- Fixed the issue where some commands did not judge the return value of RocksDB.[#2187](https://github.com/OpenAtomFoundation/pika/pull/2187)@[callme-taota](https://github.com/callme-taota)

- Fixed the problem that some command caches did not take effect due to flag calculation errors.[#2217](https://github.com/OpenAtomFoundation/pika/pull/2217) @[lqxhub](https://github.com/lqxhub)

- Fixed the problem that in master-slave replication mode, after the master instance flushdb, the slave instance cannot be accessed due to deadlock.[#2249](https://github.com/OpenAtomFoundation/pika/pull/2249)@[ForestLH](https://github.com/ForestLH)

- Fixed the issue where some commands did not judge the return value of RocksDB.[#2187](https://github.com/OpenAtomFoundation/pika/pull/2187)@[callme-taota](https://github.com/callme-taota)

- Fix the problem of info keyspace returning wrong results.[#2369](https://github.com/OpenAtomFoundation/pika/pull/2369)@[Mixficsol](https://github.com/Mixficsol)

- Standard function return value and initial value.[#2176](https://github.com/OpenAtomFoundation/pika/pull/2176)@[Mixficsol](https://github.com/Mixficsol)

- Fixed the problem of inaccurate network monitoring indicator statistics.[#2234](https://github.com/OpenAtomFoundation/pika/pull/2234)@[chengyu-l](https://github.com/chengyu-l)

- Fixed an issue where some parameters in configuration file loading were abnormal.[#2218](https://github.com/OpenAtomFoundation/pika/pull/2218)@[jettcc](https://github.com/jettcc)

- Fix Codis dashboard cpu used 100%.[#2393](https://github.com/OpenAtomFoundation/pika/pull/2393)@[chengyu-l](https://github.com/chengyu-l)

- Fix the problem of abnormal display of master and slave roles in Codis fe of pika.[#2387](https://github.com/OpenAtomFoundation/pika/pull/2387)@[chengyu-l](https://github.com/chengyu-l)

- Fix the problem of data inconsistency after migrating data.[#2485](https://github.com/OpenAtomFoundation/pika/pull/2485)@[chejinge](https://github.com/chejinge)

- Fix dbsize calculation error problem.[#2494](https://github.com/OpenAtomFoundation/pika/pull/2494)@[chejinge](https://github.com/chejinge)

- Fixed the issue of inaccurate display of the Codis-dashboard interface after scaling up or down or starting and stopping pods.[#2475](https://github.com/OpenAtomFoundation/pika/pull/2475)@[chengyu-l](https://github.com/chengyu-l)

- Fix the problem of repeated locking of DB layer.[#2372](https://github.com/OpenAtomFoundation/pika/pull/2372) @[Mixficsol](https://github.com/Mixficsol)

- Fixed the problem of data loss caused by failure to perform full copy.[#2439](https://github.com/OpenAtomFoundation/pika/pull/2439)@[wangshao1](https://github.com/wangshao1)

- Fixed the problem that during the master-slave replication process, the master instance did not correctly respond to the slave's synchronization request when executing bgsave.[#2437](https://github.com/OpenAtomFoundation/pika/pull/2437)@[wangshao1](https://github.com/wangshao1)

- During the full copy process, add data synchronization status to clarify the data synchronization progress.[#2430](https://github.com/OpenAtomFoundation/pika/pull/2430)@[baixin01](https://github.com/baixin01)

- Fixed the issue where the slave database did not lock the key of the operation when applying binlog, resulting in data inconsistency.[#2409](https://github.com/OpenAtomFoundation/pika/pull/2490) @[chejinge](https://github.com/chejinge)

- Fix the problem of master instance coredump during codis slot migration process.[#2415](https://github.com/OpenAtomFoundation/pika/pull/2415) @[chejinge](https://github.com/chejinge)

- Fixed the problem of deleting the dump file being used during the master-slave replication process.[#2377](https://github.com/OpenAtomFoundation/pika/pull/2377)@[wangshao1](https://github.com/wangshao1)

- Fixed the problem of rsync response error from slave instance during master-slave replication process.[#2319](https://github.com/OpenAtomFoundation/pika/pull/2319)@[wangshao1](https://github.com/wangshao1)

- Fixed the problem that in master-slave replication mode, after the master instance flushdb, the slave instance cannot be accessed due to deadlock.[#2372](https://github.com/OpenAtomFoundation/pika/pull/2372) @[Mixficsol](https://github.com/Mixficsol)


# v3.5.2

## New features

- Pika supports Redis transactions.  [#2124](https://github.com/OpenAtomFoundation/pika/pull/2124) @[ForestLH](https://github.com/ForestLH)

- Pika adds a cache layer for hot and cold data separation, improving read performance. [#2133](https://github.com/OpenAtomFoundation/pika/pull/2133) @[chejinge](https://github.com/chejinge)@[Mixficsol](https://github.com/Mixficsol)

- Codis-Proxy supports dynamic configuration parameter modification.  [#2103](https://github.com/OpenAtomFoundation/pika/pull/2103) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- Completing Go Test cases.  [#2063](https://github.com/OpenAtomFoundation/pika/pull/2063)@[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- Adds cache to improve compilation speed on CI.  [#2088](https://github.com/OpenAtomFoundation/pika/pull/2088)@[baerwang](https://github.com/baerwang)

## bugfix

- Fixed coredump issue when using SETRANGE command in Pika.  [#2125](https://github.com/OpenAtomFoundation/pika/issues/2125) @[chejinge](https://github.com/chejinge)

- Fixed full replication issue caused by deleting Clearreplicationid from binlog.  [#2136](https://github.com/OpenAtomFoundation/pika/issues/2136) @[Mixficsol](https://github.com/Mixficsol)

- Modified lock granularity to improve binlog writing performance in Pika.  [#2129](https://github.com/OpenAtomFoundation/pika/pull/2129) @[wangshao1](https://github.com/wangshao1)

- Added redis-copy traffic replication tool.  [#2044](https://github.com/OpenAtomFoundation/pika/issues/2044) @[Mixficsol](https://github.com/Mixficsol)

- Fixed potential data overflow issue in complex data types member variables.  [#2016](https://github.com/OpenAtomFoundation/pika/pull/2106)  @[u6th9d](https://github.com/u6th9d)

- Fixed incorrect return value issue in decr command. [#2092](https://github.com/OpenAtomFoundation/pika/pull/2092) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- Fixed issue where SETRANGE and SETBIT commands did not retain the original key's expiration time. [#2095](https://github.com/OpenAtomFoundation/pika/pull/2095) @[u6th9d](https://github.com/u6th9d)



# v3.5.1

## New features

- slow log Adds statistics on queue waiting time [#1997](https://github.com/OpenAtomFoundation/pika/pull/1997) @[wangshao1](https://github.com/wangshao1)

- ReplicationID is used for primary/secondary replication [#1951](https://github.com/OpenAtomFoundation/pika/pull/1951) @[Mixficsol](https://github.com/Mixficsol)

- WAL uses the disablewal command to support dynamic shutdown [#2015](https://github.com/OpenAtomFoundation/pika/pull/2015) @[Mixficsol](https://github.com/Mixficsol)

- The number of threads flushed and the number of threads compaction dynamically adjust into one [#2014](flush线程数和compaction线程数动态调整，合二为一) @[Tianpingan](https://github.com/Tianpingan)

- The RocksDB version was upgraded to v8.3.3 [#1999](https://github.com/OpenAtomFoundation/pika/issues/1999) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- Added documentation for configuring Pika in Macos environment [#2003](https://github.com/OpenAtomFoundation/pika/pull/2003) @[klboke](https://github.com/klboke)

- Added the ability to periodically print the length of the work queue to quickly locate problems when the queue is blocked [#1978](https://github.com/OpenAtomFoundation/pika/pull/1978) @[Tianpingan](https://github.com/Tianpingan)

- Added an indicator to detect the entire cluster using a `pika_exporter` [#1953](https://github.com/OpenAtomFoundation/pika/pull/1953) @[chenbt-hz](https://github.com/chenbt-hz)

- Realize automatic registration of Pika service on K8s environment, and automatically register at startup, so as to realize self-organization of cluster [#1931](https://github.com/OpenAtomFoundation/pika/pull/1931) @[machinly](https://github.com/machinly)


## bugfix

- Reduces unnecessary log printing by the exporter, reducing CPU utilization [#1945](https://github.com/OpenAtomFoundation/pika/pull/1945) @[Mixficsol](https://github.com/Mixficsol)

- The sentinel mechanism has been upgraded to carry out logical deletion of primary nodes that fall offline [#1949](https://github.com/OpenAtomFoundation/pika/pull/1949) @[Mixficsol](https://github.com/Mixficsol)

- Adjust the rate_limit parameter to fix the situation that RPS is 0 during pressure measurement [#2009](https://github.com/OpenAtomFoundation/pika/pull/2009) @[chejinge](https://github.com/chejinge)

- Fixed the logical determination of empty path when traversing data files in INFODATA command [#1996](https://github.com/OpenAtomFoundation/pika/pull/1996) @[Mixficsol](https://github.com/Mixficsol)

- Improved the description of some missing parts of the document [#1962](https://github.com/OpenAtomFoundation/pika/pull/1962) @[baerwang](https://github.com/baerwang)

- Use make-j to improve build speed [#1933](https://github.com/OpenAtomFoundation/pika/pull/1933) @[xiezheng-XD](https://github.com/xiezheng-XD)

- Fixed an issue where large burrs appeared on the Codis line [#2016](https://github.com/OpenAtomFoundation/pika/pull/2016) @[chejinge](https://github.com/chejinge)

- Fixed an issue where tools could not be compiled in Macos [#2011](https://github.com/OpenAtomFoundation/pika/pull/2011) @[A2ureStone](https://github.com/A2ureStone)



# v3.5.0

## Major features

### 1.Removal of Rsync

* In v3.5.0, we removed Rsync and replaced it with a self-developed full synchronization scheme, which has the features of breakpoint continuation, rate limit, file inspection and so on

  [#1805](https://github.com/OpenAtomFoundation/pika/pull/1805)  [#1591](https://github.com/OpenAtomFoundation/pika/issues/1591) [#1816](https://github.com/OpenAtomFoundation/pika/pull/1816) [#1818](https://github.com/OpenAtomFoundation/pika/pull/1818) [#1820](https://github.com/OpenAtomFoundation/pika/pull/1820)

  [@wangshao1](https://github.com/wangshao1) [@chejinge](https://github.com/chejinge) [@luky116](https://github.com/luky116)

1. Remove Rsync.
2. Implement breakpoint resuming, rate limitation, and file verification.
3. When synchronizing master-slave in Pika, perform the master run_id verification.

### 2.Compatibility with More Redis Commands

* More Redis commands are compatible and supported in v3.5.0.

  [#1273](https://github.com/OpenAtomFoundation/pika/pull/1273) [#1660](https://github.com/OpenAtomFoundation/pika/pull/1660) [#1245](https://github.com/OpenAtomFoundation/pika/pull/1245) [#1548](https://github.com/OpenAtomFoundation/pika/pull/1548) [#1843](https://github.com/OpenAtomFoundation/pika/pull/1843)

  [@wanghenshui](https://github.com/wanghenshui)  [@Mixficsol](https://github.com/Mixficsol) [@weedge](https://github.com/weedge) [@cheniujh](https://github.com/cheniujh)

1. Support the UNLINK command.
2. Support the INFO COMMANDSTATS command.
3. Support the HELLO and SETNAME commands.
4. Support the BLPOP and BRPOP commands.
5. Added Pika original command DISKRECOVERY

### 3.RocksDB Version Upgrade and Tiered Compression

* In v3.5.0, we upgraded RocksDB to version v8.1.1 and implemented tiered compression.

  [#1396](https://github.com/OpenAtomFoundation/pika/pull/1396) [#1365](https://github.com/OpenAtomFoundation/pika/pull/1365)  [#1189](https://github.com/OpenAtomFoundation/pika/pull/1189)

  [@lqxhub](https://github.com/lqxhub)  [@chejinge](https://github.com/chejinge)

1. Upgraded RocksDB version to v8.1.1.
2. Implemented tiered compression for RocksDB.
3. Added the RocksDB cache configuration option "num-shard-bits" to read from the configuration file.

### 4.Support for BlobDB:

* In v3.5.0, we added support for BlobDB KV separation.

  [#1456](https://github.com/OpenAtomFoundation/pika/pull/1456)

  [@lqxhub](https://github.com/lqxhub)

1. With this update, Pika now supports the separation of KV (Key-Value) data using BlobDB.

### 5.Codis Cluster Mode based on Codis

* In v3.5.0, we introduced the Codis Cluster Mode and added support for the command to migrate Codis slots.

  [#1279](https://github.com/OpenAtomFoundation/pika/pull/1279) [#1311](https://github.com/OpenAtomFoundation/pika/pull/1311) [#1492](https://github.com/OpenAtomFoundation/pika/pull/1492) [#1662](https://github.com/OpenAtomFoundation/pika/pull/1662) [#1682](https://github.com/OpenAtomFoundation/pika/pull/1682) [#1632](https://github.com/OpenAtomFoundation/pika/pull/1632)  [#1700](https://github.com/OpenAtomFoundation/pika/pull/1700)

  [@WyattJia](https://github.com/WyattJia) [@chejinge](https://github.com/chejinge) [@luky116](https://github.com/luky116) [@yaoyinnan](https://github.com/yaoyinnan)

1. Introducing Codis into Pika.
2. Introducing Codis CI into the project.
3. Supporting the command for Codis slot migration.
4. Adding the "slotmigrate" status indicator for whether it is in the process of reloading.

### 6.Observability

* In v3.5.0, we introduced the `pika_exporter` tool to monitor the observability metrics of Pika. Subsequently, we added several new metrics to enhance Pika's observability.

  [#1388](https://github.com/OpenAtomFoundation/pika/pull/1388)  [#1733](https://github.com/OpenAtomFoundation/pika/pull/1733)  [#1751](https://github.com/OpenAtomFoundation/pika/pull/1751)  [#1736](https://github.com/OpenAtomFoundation/pika/pull/1736) [#1560](https://github.com/OpenAtomFoundation/pika/pull/1560)  [#1696](https://github.com/OpenAtomFoundation/pika/pull/1696)

  [@chenbt-hz](https://github.com/chenbt-hz)  [@yaoyinnan](https://github.com/yaoyinnan) [@Mixficsol](https://github.com/Mixficsol)

1. Added Pika observability system `pika_exporter`.
2. Introduced monitoring metrics for network I/O traffic.
3. Added metrics for tracking command execution durations.
4. Introduced "estimate_pending_compaction_bytes" metric to analyze fragmentation.
5. Added RocksDB metrics.

### 7.Cloud-Native Deployment

* In v3.5.0, we introduced an MVP version of pika-operator, which focuses on enabling the quick deployment of a single-instance Pika service on Kubernetes.

  [#1243](https://github.com/OpenAtomFoundation/pika/pull/1243) [#1330](https://github.com/OpenAtomFoundation/pika/pull/1330) [#1347](https://github.com/OpenAtomFoundation/pika/pull/1347)

  [@machinly](https://github.com/machinly)

1. Implemented fast deployment of a single-instance Pika service on Kubernetes.
2. Implemented Pika deployment in the MiniKube environment.
3. Added End-to-End (E2E) tests to the pika-operator.

### 8.Cross-platform compilation

* In v3.5.0, Pika supports cross-platform compilation and can be compiled and used on MacOS, CentOS, and Ubuntu platforms.

  [#1372](https://github.com/OpenAtomFoundation/pika/pull/1372)  [#1488](https://github.com/OpenAtomFoundation/pika/pull/1488)

  [@loveyacper](https://github.com/loveyacper)  [@Mixficsol](https://github.com/Mixficsol)

1. Supports MacOS platform

### 9.Multi-platform CI, Go integration tests, TCL unit tests, PythonE2E tests, CTest unit tests

* In v3.5.0, several testing and integration features were added to ensure Pika's robustness across multiple platforms.

  [#1534](https://github.com/OpenAtomFoundation/pika/pull/1534) [#1772](https://github.com/OpenAtomFoundation/pika/pull/1772)  [#1769](https://github.com/OpenAtomFoundation/pika/pull/1769)  [#1347](https://github.com/OpenAtomFoundation/pika/pull/1347)  [#1268](https://github.com/OpenAtomFoundation/pika/pull/1268)  [#1693](https://github.com/OpenAtomFoundation/pika/pull/1693) [#1362](https://github.com/OpenAtomFoundation/pika/pull/1362)  [#1538](https://github.com/OpenAtomFoundation/pika/pull/1538)  [#1246](https://github.com/OpenAtomFoundation/pika/pull/1246)   [#1357](https://github.com/OpenAtomFoundation/pika/pull/1357)  [#1534](https://github.com/OpenAtomFoundation/pika/pull/1534) [#1772](https://github.com/OpenAtomFoundation/pika/pull/1772)

  [@machinly](https://github.com/machinly)  [@wanghenshui](https://github.com/wanghenshui) [@chenbt-hz](https://github.com/chenbt-hz)  [@Tangruilin](https://github.com/Tangruilin)  [@kernelai](https://github.com/kernelai) [@Mixficsol](https://github.com/Mixficsol)

1. Added CI on CentOS environment to ensure compatibility on this platform.
2. Added CI on MacOS environment to validate Pika's behavior on MacOS.
3. Introduced an End-to-End (E2E) testing framework to comprehensively test Pika's functionality from end to end.
4. Integrated CMake build environment in Github CI Workflow to facilitate the compilation process.
5. Implemented a "populate" method in TCL scripts to simulate Redis debug populate method for test data population.
6. Introduced a script to enable easy execution of unit tests.
7. Added CMake files in Blackwidow and included unit tests for Blackwidow.
8. Utilized CTest for conducting unit tests to ensure code correctness.
9. Ported Redis testing scripts to Pika to leverage existing tests and verify Pika's compatibility with Redis.
10. Included new tasks in the CI pipeline to verify successful compilation of Pika on CentOS systems.

# Features

- Added automatic rate limiting for "compact" operation to reduce its impact on the latency of upper-layer data read and write operations. [#1374](https://github.com/OpenAtomFoundation/pika/pull/1374) [@wanghenshui](https://github.com/wanghenshui)
- Introduced the "aof_to_pika" toolset for data conversion. [#1340](https://github.com/OpenAtomFoundation/pika/pull/1340) [@Axlgrep](https://github.com/Axlgrep)
- Implemented the printing of the Pika logo. [#1787](https://github.com/OpenAtomFoundation/pika/pull/1787) [@Mixficsol](https://github.com/Mixficsol)
- Optimized certain code portions using clang-tidy. [#1701](https://github.com/OpenAtomFoundation/pika/pull/1701) [#1730](https://github.com/OpenAtomFoundation/pika/pull/1730) [@longfar-ncy](https://github.com/longfar-ncy)
- Refactored C++98-style code to C++11-style following C++ coding guidelines. [#1684](https://github.com/OpenAtomFoundation/pika/pull/1684) [@chejinge](https://github.com/chejinge)
- Introduced support for the "snappy" library. [#1216](https://github.com/OpenAtomFoundation/pika/pull/1216) [@kernelai](https://github.com/kernelai)
- Added CLA (Contributor License Agreement) file. [#1260](https://github.com/OpenAtomFoundation/pika/pull/1260) [@kernelai](https://github.com/kernelai)
- Extended memory-related configuration options to support units in K, M, or G. [#1307](https://github.com/OpenAtomFoundation/pika/pull/1307) [@lqxhub](https://github.com/lqxhub)
- Added libunwind as a dependency library. [#1316](https://github.com/OpenAtomFoundation/pika/pull/1316) [@kernelai](https://github.com/kernelai)
- Included a compilation script for Pika. [#1356](https://github.com/OpenAtomFoundation/pika/pull/1356) [@lqxhub](https://github.com/lqxhub)
- Added "rate-limiter-bandwidth" option to pika.conf for rate limiting. [#1272](https://github.com/OpenAtomFoundation/pika/pull/1272) [@wanghenshui](https://github.com/wanghenshui)
- Enhanced the "info" command by adding "redis_version" information. [#1403](https://github.com/OpenAtomFoundation/pika/pull/1403) [@wanghenshui](https://github.com/wanghenshui)
- Provided CMake support for aof_to_pika tool. [#1436](https://github.com/OpenAtomFoundation/pika/pull/1436) [@A2ureStone](https://github.com/A2ureStone)
- Introduced clang-formatted shell scripts. [#1448](https://github.com/OpenAtomFoundation/pika/pull/1448) [@lqxhub](https://github.com/lqxhub)
- Added a configuration option to disable the compilation of command docs. [#1642](https://github.com/OpenAtomFoundation/pika/pull/1642) [@tedli](https://github.com/tedli)
- Provided CMake support for benchmark_client, binlog_sender, manifest_generator, rdb_to_pika, txt_to_pika, pika_to_txt, and pika_port. [#1451](https://github.com/OpenAtomFoundation/pika/pull/1451) [@A2ureStone](https://github.com/A2ureStone)

# Improvement

- Removed sharding mode from Pika for uniformity. [#1481](https://github.com/OpenAtomFoundation/pika/pull/1481) [@chejinge](https://github.com/chejinge)
- Added slot numbers to the configuration (config) settings. [#1664](https://github.com/OpenAtomFoundation/pika/pull/1664) [@luky116](https://github.com/luky116)
- Unified the naming of partitions as slots. [#1571](https://github.com/OpenAtomFoundation/pika/pull/1571) [#1655](https://github.com/OpenAtomFoundation/pika/pull/1655) [@chejinge](https://github.com/chejinge)
- Replaced Monitor thread with the Monitor command. [#1551](https://github.com/OpenAtomFoundation/pika/pull/1551) [@loveyacper](https://github.com/loveyacper)
- Added checks for empty and incorrect passwords during server startup. [#1645](https://github.com/OpenAtomFoundation/pika/pull/1645) [@hqh-cell](https://github.com/hqh-cell)
- Refactored pstd_mutex. [#1440](https://github.com/OpenAtomFoundation/pika/pull/1440) [@4kangjc](https://github.com/4kangjc)
- Modified the command information key-space response data. [#1450](https://github.com/OpenAtomFoundation/pika/pull/1450) [@lqxhub](https://github.com/lqxhub)
- Merged common code between pstd and storage. [#1419](https://github.com/OpenAtomFoundation/pika/pull/1419) [@4kangjc](https://github.com/4kangjc)
- Added "spop" command by count. [#1434](https://github.com/OpenAtomFoundation/pika/pull/1434) [@ChanphongGu](https://github.com/ChanphongGu)
- Provided documentation for Pika Docker images. [#1557](https://github.com/OpenAtomFoundation/pika/pull/1557) [@machinly](https://github.com/machinly)
- Replaced the old GetEnv interface with RocksDB's GetFileSystem interface. [#1213](https://github.com/OpenAtomFoundation/pika/pull/1213) [@kernelai](https://github.com/kernelai)
- Added scattered Pika documentation. [#1193](https://github.com/OpenAtomFoundation/pika/pull/1193) [@fengpeiyuan](https://github.com/fengpeiyuan)
- Upgraded CMake to version 3.18. [#1366](https://github.com/OpenAtomFoundation/pika/pull/1366) [@AlexStocks](https://github.com/AlexStocks)
- Added a format tool to Pika. [#1512](https://github.com/OpenAtomFoundation/pika/pull/1512) [@liuqian1990](https://github.com/liuqian1990)
- Added clang-tidy support to Pika. [#1515](https://github.com/OpenAtomFoundation/pika/pull/1515) [#1730](https://github.com/OpenAtomFoundation/pika/pull/1730) [@ForestLH](https://github.com/ForestLH) [@longfar-ncy](https://github.com/longfar-ncy)
- Launched Pika and pika-operator in MiniKube. [#1330](https://github.com/OpenAtomFoundation/pika/pull/1330) [@machinly](https://github.com/machinly)
- Applied Google coding style to clang-format. [#1479](https://github.com/OpenAtomFoundation/pika/pull/1479) [@Mixficsol](https://github.com/Mixficsol)
- Refactored pstd/env using std::filesystem. [#1470](https://github.com/OpenAtomFoundation/pika/pull/1470) [@4kangjc](https://github.com/4kangjc)
- Added Slice to pstd::GetFixed. [#1425](https://github.com/OpenAtomFoundation/pika/pull/1425) [@4kangjc](https://github.com/4kangjc)
- Used inline variables in header files. [#1407](https://github.com/OpenAtomFoundation/pika/pull/1407) [@4kangjc](https://github.com/4kangjc)
- Formatted code using clang-formats. [#1378](https://github.com/OpenAtomFoundation/pika/pull/1378) [@AlexStocks](https://github.com/AlexStocks)
- Moved pika-operator to the pika-tools directory. [#1468](https://github.com/OpenAtomFoundation/pika/pull/1468) [@luky116](https://github.com/luky116)
- Added Codis to Pika migration tool. [#1514](https://github.com/OpenAtomFoundation/pika/pull/1514) [@chenbt-hz](https://github.com/chenbt-hz)

# Bugfixes

- Fixed coredump caused by incompatible CPU instruction sets. [#1812](https://github.com/OpenAtomFoundation/pika/pull/1812) [@chejinge](https://github.com/chejinge)
- Fixed abnormal exit issue in the Monitor command. [#1804](https://github.com/OpenAtomFoundation/pika/pull/1804) [@Mixficsol](https://github.com/Mixficsol)
- Fixed incomplete configuration overwrite. [#1800](https://github.com/OpenAtomFoundation/pika/pull/1800) [@yaoyinnan](https://github.com/yaoyinnan)
- Fixed CTest tests on MacOS. [#1789](https://github.com/OpenAtomFoundation/pika/pull/1789) [#1721](https://github.com/OpenAtomFoundation/pika/pull/1721) [#1782](https://github.com/OpenAtomFoundation/pika/pull/1782) [@tedli](https://github.com/tedli) [@Mixficsol](https://github.com/Mixficsol)
- Fixed loading master-run-id from the configuration file during process startup, which caused the need for re-syncing historical data on slave nodes. [#1778](https://github.com/OpenAtomFoundation/pika/pull/1778) [@luky116](https://github.com/luky116)
- Fixed format error in overrides option during Pika single test. [#1768](https://github.com/OpenAtomFoundation/pika/pull/1768) [@A2ureStone](https://github.com/A2ureStone)
- Fixed command to retrieve INFO xxx (INFO data, INFO stats) by index instead of using INFO ALL. [#1762](https://github.com/OpenAtomFoundation/pika/pull/1762) [@yaoyinnan](https://github.com/yaoyinnan)
- Improved instantaneous_metric to collect network metrics every 5 seconds. [#1757](https://github.com/OpenAtomFoundation/pika/pull/1757) [@yaoyinnan](https://github.com/yaoyinnan)
- Improved config get to return complete parameters. [#1593](https://github.com/OpenAtomFoundation/pika/issues/1593) [@luky116](https://github.com/luky116)
- Fixed possible empty slot issue. [#1712](https://github.com/OpenAtomFoundation/pika/pull/1712) [@Mixficsol](https://github.com/Mixficsol)
- Fixed some bugs related to version selection in the makefile. [#1714](https://github.com/OpenAtomFoundation/pika/pull/1714) [@luky116](https://github.com/luky116)
- Fixed bug in executing HSET for the first time. [#1710](https://github.com/OpenAtomFoundation/pika/pull/1710) [@Mixficsol](https://github.com/Mixficsol)
- Fixed the issue of binlog offset on slave nodes being greater than master nodes, causing TrySync failure. [#1681](https://github.com/OpenAtomFoundation/pika/issues/1681) [@luky116](https://github.com/luky116)
- Fixed a NOAUTH error test, where any command would give an error when AUTH is required. [#1680](https://github.com/OpenAtomFoundation/pika/pull/1680) [@hqh-cell](https://github.com/hqh-cell)
- Fixed the issue of binlog files not being automatically cleaned up when exceeding the limit. [#1679](https://github.com/OpenAtomFoundation/pika/pull/1679) [@luky116](https://github.com/luky116)
- Fixed issues with LPUSHX and RPUSHX multi-element insertion. [#1525](https://github.com/OpenAtomFoundation/pika/pull/1525) [@ForestLH](https://github.com/ForestLH)
- Fixed error in executing the BGSAVE command. [#1523](https://github.com/OpenAtomFoundation/pika/pull/1523) [@Brokenice0415](https://github.com/Brokenice0415)
- Fixed the use of io_uring when closing RocksDB to prevent errors during linking. [#1489](https://github.com/OpenAtomFoundation/pika/pull/1489) [@lqxhub](https://github.com/lqxhub)
- Fixed MacOS environment Pika compilation warnings. [#1740](https://github.com/OpenAtomFoundation/pika/pull/1740) [@Mixficsol](https://github.com/Mixficsol)
- Fixed error message for unsupported commands, adapted for go-redis. [#1244](https://github.com/OpenAtomFoundation/pika/pull/1244) [@wgqi1126](https://github.com/wgqi1126)
- Fixed a protobuf3 compilation warning. [#1267](https://github.com/OpenAtomFoundation/pika/pull/1267) [@wanghenshui](https://github.com/wanghenshui)
- Fixed bugs in EXISTS and DEL commands in sharding mode. [#1277](https://github.com/OpenAtomFoundation/pika/pull/1277) [@wanghenshui](https://github.com/wanghenshui)
- Fixed rsync auth bug. [#1278](https://github.com/OpenAtomFoundation/pika/pull/1278) [@wanghenshui](https://github.com/wanghenshui)
- Fixed rewrite result format to comply with Redis standards. [#1339](https://github.com/OpenAtomFoundation/pika/pull/1339) [@lqxhub](https://github.com/lqxhub)
- Fixed SET NX/XX return values to match Redis behavior. [#1343](https://github.com/OpenAtomFoundation/pika/pull/1343) [@gtygo](https://github.com/gtygo)
- Fixed missing pika_master_link_status and pika_slave_priority indicators in exporter. [#1726](https://github.com/OpenAtomFoundation/pika/pull/1726) [@Mixficsol](https://github.com/Mixficsol)
- Fixed potential coredump when Pika uses too many file descriptors, preventing it from exceeding the ulimit. [#1346](https://github.com/OpenAtomFoundation/pika/pull/1346) [@kernelai](https://github.com/kernelai)
- Fixed initialization of group members in classes to prevent unexpected closure of the connection between redis-cli and Pika. [#1390](https://github.com/OpenAtomFoundation/pika/pull/1390) [@AlexStocks](https://github.com/AlexStocks)
- Fixed delete_dir snprintf bug. [#1400](https://github.com/OpenAtomFoundation/pika/pull/1400) [@wanghenshui](https://github.com/wanghenshui)
- Fixed thread num limitation. [#1401](https://github.com/OpenAtomFoundation/pika/pull/1401) [@wanghenshui](https://github.com/wanghenshui)
- Fixed redis-benchmark to get config from Pika. [#1402](https://github.com/OpenAtomFoundation/pika/pull/1402) [@wanghenshui](https://github.com/wanghenshui)
- Fixed initialization of class members. [#1406](https://github.com/OpenAtomFoundation/pika/pull/1406) [@AlexStocks](https://github.com/AlexStocks)
- Fixed issues in Pika's master-slave synchronization tests on MacOS. [#1776](https://github.com/OpenAtomFoundation/pika/pull/1776) [@Mixficsol](https://github.com/Mixficsol)
- Fixed NULL to nullptr. [#1408](https://github.com/OpenAtomFoundation/pika/pull/1408) [@AlexStocks](https://github.com/AlexStocks)
- Fixed size boundary of ttl to set the expiration timestamp. [#1413](https://github.com/OpenAtomFoundation/pika/pull/1413) [@ForestLH](https://github.com/ForestLH)
- Fixed access to nullptr. [#1414](https://github.com/OpenAtomFoundation/pika/pull/1414) [#1426](https://github.com/OpenAtomFoundation/pika/pull/1426) [@fly1ngpengu1ns](https://github.com/fly1ngpengu1ns) [@Pikachu1412](https://github.com/Pikachu1412)
- Fixed Monitor command. [#1424](https://github.com/OpenAtomFoundation/pika/pull/1424) [@loveyacper](https://github.com/loveyacper)
- Fixed partitionInfo::operator to return a boolean value. [#1427](https://github.com/OpenAtomFoundation/pika/pull/1427) [@4kangjc](https://github.com/4kangjc)
- Fixed access to nullptr in src/net. [#1453](https://github.com/OpenAtomFoundation/pika/pull/1453) [@gitveg](https://github.com/gitveg)
- Fixed c++17 build failure in aof_reader. [#1454](https://github.com/OpenAtomFoundation/pika/pull/1454) [@lqxhub](https://github.com/lqxhub)
- Fixed compilation warnings. [#1455](https://github.com/OpenAtomFoundation/pika/pull/1455) [@wangchuande](https://github.com/wangchuande)
- Fixed occasional build failure in operator. [#1464](https://github.com/OpenAtomFoundation/pika/pull/1464) [@machinly](https://github.com/machinly)
- Fixed CondVar to allow exiting. [#1475](https://github.com/OpenAtomFoundation/pika/pull/1475) [@lqxhub](https://github.com/lqxhub)
- Fixed libfmt debug build name error. [#1476](https://github.com/OpenAtomFoundation/pika/pull/1476) [@lqxhub](https://github.com/lqxhub)
- Fixed abnormal exit issue in the Monitor command. [#1804](https://github.com/OpenAtomFoundation/pika/pull/1804) [@Mixficsol](https://github.com/Mixficsol)
- Fixed abnormal binlog offset issue on slave nodes. [#1681](https://github.com/OpenAtomFoundation/pika/issues/1681) [@luky116](https://github.com/luky116)
- Fixed "no auth" error in unit tests. [#1680](https://github.com/OpenAtomFoundation/pika/pull/1680) [@hqh-cell](https://github.com/hqh-cell)
- Fixed issue where binlog files exceeding the limit were not being deleted. [#1679](https://github.com/OpenAtomFoundation/pika/pull/1679) [@luky116](https://github.com/luky116)
- Fixed string-related unit tests. [#1675](https://github.com/OpenAtomFoundation/pika/pull/1675) [@Mixficsol](https://github.com/Mixficsol)
- Fixed issue where tmp files were not deleted during abnormal exit in Codis. [#1661](https://github.com/OpenAtomFoundation/pika/issues/1661) [@yaoyinnan](https://github.com/yaoyinnan)
- Fixed inconsistency in data replication between master and slave. [#1638](https://github.com/OpenAtomFoundation/pika/issues/1638) [@cheniujh](https://github.com/cheniujh)
- Fixed failure to retrieve Metrics indicators in the INFO command. [#1646](https://github.com/OpenAtomFoundation/pika/issues/1646) [@chejinge](https://github.com/chejinge)
- Fixed a type error in basic type on one side. [#1625](https://github.com/OpenAtomFoundation/pika/pull/1625) [@chenbt-hz](https://github.com/chenbt-hz)
- Fixed incorrect parameter prompts for LPOP and RPOP interfaces. [#1520](https://github.com/OpenAtomFoundation/pika/issues/1520) [@luky116](https://github.com/luky116)
- Fixed the issue where Pika remained in an erroneous state after clearing data due to the instance's disk being full, requiring a restart to recover. [#1765](https://github.com/OpenAtomFoundation/pika/pull/1765) [#1822](https://github.com/OpenAtomFoundation/pika/pull/1822) [#1843](https://github.com/OpenAtomFoundation/pika/pull/1843) [@Yangsx-1](https://github.com/Yangsx-1) [@Mixficsol](https://github.com/Mixficsol)

# Notice

* Removed the Proxy feature introduced in version 3.4. [#1274](https://github.com/OpenAtomFoundation/pika/pull/1274) [@wanghenshui](https://github.com/wanghenshui)
* Removed the submodule. [#1312](https://github.com/OpenAtomFoundation/pika/pull/1312) [@wanghenshui](https://github.com/wanghenshui)
* Removed the sharding command. [#1469](https://github.com/OpenAtomFoundation/pika/pull/1469) [@chejinge](https://github.com/chejinge)
* Removed tcmalloc. [#1472](https://github.com/OpenAtomFoundation/pika/pull/1472) [@Mixficsol](https://github.com/Mixficsol)
* Added protect mode for the leader. [#1276](https://github.com/OpenAtomFoundation/pika/pull/1276) [@wanghenshui](https://github.com/wanghenshui)
* Synchronized the latest code from Codis to the Pika repository. [#1279](https://github.com/OpenAtomFoundation/pika/pull/1279) [@WyattJia](https://github.com/WyattJia)
* Added external dependency libraries in CMake files. [#1281](https://github.com/OpenAtomFoundation/pika/pull/1281) [@kernelai](https://github.com/kernelai)
* Added a list of external dependency libraries in CMake files. [#1285](https://github.com/OpenAtomFoundation/pika/pull/1285) [@kernelai](https://github.com/kernelai)
* Created a static link library for Pika, allowing it to be directly linked with dependent libraries to form Pika's binary package. [#1310](https://github.com/OpenAtomFoundation/pika/pull/1310) [@Tangruilin](https://github.com/Tangruilin)
* Made protobuf generation independent of the local environment, enabled multi-core compilation, and fixed the pthread error on Debian. [#1314](https://github.com/OpenAtomFoundation/pika/pull/1314) [@lqxhub](https://github.com/lqxhub)
* Renamed "slash" to "pstd". [#1348](https://github.com/OpenAtomFoundation/pika/pull/1348) [@AlexStocks](https://github.com/AlexStocks)
* Renamed the "src/pink" directory to "src/net". [#1350](https://github.com/OpenAtomFoundation/pika/pull/1350) [@AlexStocks](https://github.com/AlexStocks)
* Renamed the words "bw" and "blackwidow" to "storage". [#1352](https://github.com/OpenAtomFoundation/pika/pull/1352) [#1353](https://github.com/OpenAtomFoundation/pika/pull/1353) [@AlexStocks](https://github.com/AlexStocks)
* Updated the compiler requirement documentation. [#1420](https://github.com/OpenAtomFoundation/pika/pull/1420) [@4kangjc](https://github.com/4kangjc)
* Replaced invalid links. [#1430](https://github.com/OpenAtomFoundation/pika/pull/1430) [#1441](https://github.com/OpenAtomFoundation/pika/pull/1441) [#1471](https://github.com/OpenAtomFoundation/pika/pull/1471) [@gitveg](https://github.com/gitveg) [@1321034767](https://github.com/1321034767) [@chiehwarm](https://github.com/chiehwarm)
* Upgraded Redis from 4.3.3 to 4.4.4 in /tools/codis2pika/test. [#1536](https://github.com/OpenAtomFoundation/pika/pull/1536) [@dependabot](https://chat.openai.com/)
* Upgraded golang.org/x/sys from 0.0.0-20210927094055-39ccf1dd6fa6 to 0.1.0 in /tools/codis2pika. [#1535](https://github.com/OpenAtomFoundation/pika/pull/1535) [@dependabot](https://chat.openai.com/)
* Replaced new/delete with smart pointers. [#1503](https://github.com/OpenAtomFoundation/pika/pull/1503) [#1502](https://github.com/OpenAtomFoundation/pika/pull/1502) [#1493](https://github.com/OpenAtomFoundation/pika/pull/1493) [@cheniujh](https://github.com/cheniujh) [@A2ureStone](https://github.com/A2ureStone) [@iiiuwioajdks](https://github.com/iiiuwioajdks)
* Replaced fprintf with glog. [#1421](https://github.com/OpenAtomFoundation/pika/pull/1421) [@Mixficsol](https://github.com/Mixficsol)