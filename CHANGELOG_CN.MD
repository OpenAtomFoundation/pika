# v3.5.5

## New features

- 给 RTC 模型增加处理开关，可以根据开关控制是否开启 RTC 模型[#2841](https://github.com/OpenAtomFoundation/pika/pull/2841)@[cheniujh](https://github.com/cheniujh)

- 用 RTC 模型处理 Pika 访问缓存部分，提升 Pika 服务的读性能[#2837](https://github.com/OpenAtomFoundation/pika/pull/2837)@[cheniujh](https://github.com/cheniujh)

- incr、append 命令在传输 binlog 时，使用 pksetexat 命令，防止因为不正确的操作导致数据无法过期，出现脏数据[#2833](https://github.com/OpenAtomFoundation/pika/pull/2833)@[chejinge](https://github.com/chejinge)

- 添加定时删除日志的任务，默认 7 天删除一次，可以根据自己需求在 config 文件中进行配置[#2829](https://github.com/OpenAtomFoundation/pika/pull/2829)@[XiaoLiang2333](https://github.com/XiaoLiang2333)

- 将管理命令移出主线程，防止因为管理命令调用频繁或者耗时过高，阻塞主线程[#2727](https://github.com/OpenAtomFoundation/pika/pull/2727)@[chejinge](https://github.com/chejinge)

- Pika 线程整理，避免启动无用线程造成资源浪费[#2697](https://github.com/OpenAtomFoundation/pika/pull/2697)@[chejinge](https://github.com/chejinge)

- 添加 pika benchark 工具，旨在提升压测效率，并输出可视化的统计图表[#2663](https://github.com/OpenAtomFoundation/pika/pull/2663)@[luky116](https://github.com/luky116)


## Improvement

- 优化主从复制，确保 Master 端的 SlaveNode 在提交 bgsave 任务前进入 DBSync 状态，防止bgsave执行时的 binlog 在极端情况下被清除[#2798](https://github.com/OpenAtomFoundation/pika/pull/2798)@[cheniujh](https://github.com/cheniujh)

- 优化 Apply binlog 时锁机制，减少不必要的锁竞争[#2773](https://github.com/OpenAtomFoundation/pika/pull/2773)@[cheniujh](https://github.com/cheniujh)

- 添加 Geo 数据类型的 TCL 测试，并修复测试过程中遇到的 bug[#2753](https://github.com/OpenAtomFoundation/pika/pull/2753)@[saz97](https://github.com/saz97)

- 更新 Pika Docker Readme, 可以按照 Readme 在 Docker 中部署 Pika 服务[#2743](https://github.com/OpenAtomFoundation/pika/pull/2743)@[luky116](https://github.com/luky116)

- 支持对更多的 RocksDB 参数进行动态调整，用户根据不同的业务使用场景调整参数提升 Pika 的读写性能[#2728](https://github.com/OpenAtomFoundation/pika/pull/2728)@[cheniujh](https://github.com/cheniujh)

- Pkpatternmatchdel 命令支持删除 Redis Stream 数据类型[#2723](https://github.com/OpenAtomFoundation/pika/pull/2723)@[wangshao1](https://github.com/wangshao1)

- 重构主从复制模式 slave 节点的主从同步线程模型，尽可能减少 binlog 消费阻塞问题[#2638](https://github.com/OpenAtomFoundation/pika/pull/2638)@[cheniujh](https://github.com/cheniujh)

- 增加主从复制状态指标 repl_connect_status，方便运维人员清晰明确的判断当前的主从复制状态[#2656](https://github.com/OpenAtomFoundation/pika/pull/2656)@[cheniujh](https://github.com/cheniujh)

- 新增 RocksDB Compaction 策略动态调整参数，用户可以根据业务调整 Compaction 策略，降低 Compaction 操作对服务性能的损耗[#2538](https://github.com/OpenAtomFoundation/pika/pull/2735)@[wangshao1](https://github.com/wangshao1)

- 对事务新增 TCL 测试并修复测试中遇到的 bug[#2844](https://github.com/OpenAtomFoundation/pika/pull/2844)@[luky116](https://github.com/luky116)


## Bugfix

- 修改 Pika 自动化测试客户端链接 server 端的超时时间，避免因为长时间断开导致测试失败[#2863](https://github.com/OpenAtomFoundation/pika/pull/2863)@[cheniujh](https://github.com/cheniujh)

- 修复 kill client 命令杀连接流程不正确的[#2862](https://github.com/OpenAtomFoundation/pika/pull/2862)@[cheniujh](https://github.com/cheniujh)

- 修复 `blpop/brpop` 更新数据库的时候未更新缓存可能回导致 RocksDB 数据库与 RedisCache 缓存不一致的现象[#2858](https://github.com/OpenAtomFoundation/pika/pull/2858)@[cheniujh](https://github.com/cheniujh)

- 修复 Pika 不支持 Redis-Sentinel 的问题[#2854](https://github.com/OpenAtomFoundation/pika/pull/2854)@[cheniujh](https://github.com/cheniujh)

- 修改 `flushall` 的逻辑避免统一处理时，多清理一次数据[#2846](https://github.com/OpenAtomFoundation/pika/pull/2846)@[cheniujh](https://github.com/cheniujh)

- `PkPatternMatchDel` 命令在删除 DB 的同时删除 RedisCache，避免出现 RocksDB 数据库与 RedisCache 缓存数据不一致的情况[#2839](https://github.com/OpenAtomFoundation/pika/pull/2839)@[chejinge](https://github.com/chejinge)

- 用 Rocky 环境替代 CentOS, github CI 流程总体支持 MacOS/Ubuntu/Rocky 三个环境[#2823](https://github.com/OpenAtomFoundation/pika/pull/2823)@[QlQlqiqi ](https://github.com/QlQlqiqi)

- 优化读锁的范围，避免因为重复消费 binlog 导致主、从数据不一致的问题[#2818](https://github.com/OpenAtomFoundation/pika/pull/2818)@[cheniujh](https://github.com/cheniujh)

- 修改 client watch 的 key，被任何人修改（包括自己的改动），都会失效的问题[#2815](https://github.com/OpenAtomFoundation/pika/pull/2815)@[luky116](https://github.com/luky116)

- 修改 slave_priority 的默认值，防止因为运维同学没有设置该值，导致主从切换失败[#2813](https://github.com/OpenAtomFoundation/pika/pull/2813)@[chejinge](https://github.com/chejinge)

- Multi 命令更新数据库的同时更新缓存，避免读取不到数据[#2810](https://github.com/OpenAtomFoundation/pika/pull/2810)@[luky116](https://github.com/luky116)

- 修复执行 `slaveof no one` 时出现异常日志输出的问题[#2800](https://github.com/OpenAtomFoundation/pika/pull/2800)@[cheniujh](https://github.com/cheniujh)

- 修复 Pika block-cache 信息计算不准确，内存使用计算不标准的问题[#2797](https://github.com/OpenAtomFoundation/pika/pull/2797)@[bigdaronlee163](https://github.com/bigdaronlee163)

- 修改主从复制过程中 flushdb binlog 的处理逻辑，确保按照顺序执行，避免出现主从不一致的情况[#2794](https://github.com/OpenAtomFoundation/pika/pull/2794)@[cheniujh](https://github.com/cheniujh)

- 添加标志位、时间戳和返回值机制，确保 Pika 在执行 flushdb 且处理异步删除旧目录时冲突时正确处理[#2790](https://github.com/OpenAtomFoundation/pika/pull/2790)@[cheniujh](https://github.com/cheniujh)

- 修改主从复制过程中 flushdb binlog 的处理逻辑，确保按照顺序执行，避免出现主从不一致的情况[#2790](https://github.com/OpenAtomFoundation/pika/pull/2790)@[cheniujh](https://github.com/cheniujh)

- 修复 `PKPatternMatchDel` 命令未删除迭代器可能会导致 RocksDB 删除数据不彻底的问题[#2786](https://github.com/OpenAtomFoundation/pika/pull/2786)@[wangshao1](https://github.com/wangshao1)

- timerTaskThread_ 重命令为 timer_task_thread_[#2776](https://github.com/OpenAtomFoundation/pika/pull/2776)@[cheniujh](https://github.com/cheniujh)

- 修复 min-blob-size 参数解析失败，导致 kv 分离使用出错的问题[#2767](https://github.com/OpenAtomFoundation/pika/pull/2767)@[wangshao1](https://github.com/wangshao1)

- 修复 sentinel 主从切换时，由于主节点和从节点数据量不一致导致主从切换状态扭转错误问题[#2766](https://github.com/OpenAtomFoundation/pika/pull/2766)@[cheniujh](https://github.com/cheniujh)

- 修复 `Zverank` 命令计算错误，导致返回值错误的问题[#2763](https://github.com/OpenAtomFoundation/pika/pull/2763)@[chejinge](https://github.com/chejinge)

- 修复 `Pksetat` 命令更新 DB 的同时未更新 RedisCache 缓存，可能会存在 RocksDB 数据库与 RedisCache 缓存不一致的问题[#2759](https://github.com/OpenAtomFoundation/pika/pull/2759)@[chejinge](https://github.com/chejinge)

- 修复 Pika-port 传输数据过程中报错的问题[#2758](https://github.com/OpenAtomFoundation/pika/pull/2758)@[guangkun123](https://github.com/guangkun123)

- 修复 RsynClient 异常退出后，未做失败处理导致全量复制数据不完整的问题[#2756](https://github.com/OpenAtomFoundation/pika/pull/2756)@[cheniujh](https://github.com/cheniujh)

- 修复 Pika 不能批量扩容的问题[#2746](https://github.com/OpenAtomFoundation/pika/pull/2746)@[cheniujh](https://github.com/cheniujh)

- 修复参数未初始化导致 slotsscan 等命令不能和 bgsave 命令相互制衡的问题[#2745](https://github.com/OpenAtomFoundation/pika/pull/2745)@[chejinge](https://github.com/chejinge)

- 修复 SlotMigrate 返回值错误，可能会导致数据迁移中断的问题[#2741](https://github.com/OpenAtomFoundation/pika/pull/2741)@[wangshao1](https://github.com/wangshao1)

- Pksetexat 更新数据库的同时更新 RedisCache，避免出现RocksDB 数据库与 RedisCache 缓存不一致的问题[#2736](https://github.com/OpenAtomFoundation/pika/pull/2736)@[longfar-ncy](https://github.com/longfar-ncy)

- 修复 RedisCache 层因为没有使用 storage 层解析的 ttl 可能会导致 RocksDB 数据库与 RedisCache 缓存不一致的问题[#2729](https://github.com/OpenAtomFoundation/pika/pull/2729)@[chejinge](https://github.com/chejinge)

- 修复 Stream 类型数据不能被 RocksDB compaction 会导致过期数据一直存在的问题[#2724](https://github.com/OpenAtomFoundation/pika/pull/2724)@[wangshao1](https://github.com/wangshao1)

- 修复 ACL 认证可能会偶发性失败的问题[#2714](https://github.com/OpenAtomFoundation/pika/pull/2714)@[luky116](https://github.com/luky116)

- 修复 Pika cmdID 赋值在 Cmd 初始函数中，可能会导致并发构造的时候出现内存泄漏的问题[#2692](https://github.com/OpenAtomFoundation/pika/pull/2692)@[gukj-spel](https://github.com/gukj-spel)

- 修复 Spop 在写 binlog 时可能会出现竞态问题[#2674](https://github.com/OpenAtomFoundation/pika/pull/2674)@[cheniujh](https://github.com/cheniujh)

- 修复 server_stat 中的数据竞态问题[#2671](https://github.com/OpenAtomFoundation/pika/pull/2671)@[cheniujh](https://github.com/cheniujh)

- 修复多 DB 下全量同步超时后不重试的问题[#2667](https://github.com/OpenAtomFoundation/pika/pull/2667)@[cheniujh](https://github.com/cheniujh)

- 修复多 DB 主从超时场景下，可能会出现窗口崩溃的问题[#2666](https://github.com/OpenAtomFoundation/pika/pull/2666)@[cheniujh](https://github.com/cheniujh)

- 修复主从同步限速逻辑中重复解锁的问题[#2657](https://github.com/OpenAtomFoundation/pika/pull/2657)@[cheniujh](https://github.com/cheniujh)

# v4.0.0

## New features

- Pika Geo 数据类型增加 TCL 测试，并修复测试过程中遇到的缺陷[#2753](https://github.com/OpenAtomFoundation/pika/pull/2753)@[saz97](https://github.com/saz97)

- Pika 支持在 FreeBSD14 平台上进行编译打包[#2711](https://github.com/OpenAtomFoundation/pika/pull/2711)@[lqxhub](https://github.com/lqxhub)

- Pika 线程整理，避免启动过多无用线程，对不同的线程进行命名，方便问题定位[#2697](https://github.com/OpenAtomFoundation/pika/pull/2697)@[chejinge](https://github.com/chejinge)

- Mget 支持多 key 查询缓存, 记录未命中的 key 去 DB 中查询，提升 Pika 服务的读性能[#2675](https://github.com/OpenAtomFoundation/pika/pull/2675)@[chejinge](https://github.com/chejinge)

- Codis 支持 info 命令, 可以通过该命令查询 Codis-proxy 的 info 信息[#2688](https://github.com/OpenAtomFoundation/pika/pull/2688)@[chienguo](https://github.com/chienguo)

- 添加 Floyd 的 compaction-filter 的 Gtest[#2669](https://github.com/OpenAtomFoundation/pika/pull/2669)@[Mixficsol ](https://github.com/Mixficsol)

- Codis-proxy 新增 P99 P95 等监控耗时指标[#2668](https://github.com/OpenAtomFoundation/pika/pull/2668)@[chejinge](https://github.com/chejinge)

- 添加 Pika 压测指标，提升 Pika 压测效率，并输出可视化的统计图表[#2663](https://github.com/OpenAtomFoundation/pika/pull/2663)@[luky116](https://github.com/luky116)

- Pika 主从复制新增监控指标 repl_connect_status, 可以更加明确清晰的确定当前的主从复制的状态[#2638](https://github.com/OpenAtomFoundation/pika/pull/2638)@[cheniujh](https://github.com/cheniujh)

- Pika 不支持不同类型的重复 key, 写入重复 key 返回非法类型[#2609](https://github.com/OpenAtomFoundation/pika/pull/2609)@[Mixficsol](https://github.com/Mixficsol)

- 添加支持分区索引过滤的功能[#2601](https://github.com/OpenAtomFoundation/pika/pull/2601)@[vacheli](https://github.com/vacheli)

- Pika 支持第三代存储引擎 Floyd, 通过支持多 rocksdb 实例、对 Blob 的使用进行优化、对过期数据的清理进行优化，提升了 Pika 实例的读写性能[#2413](https://github.com/OpenAtomFoundation/pika/pull/2413)@[wangshao1](https://github.com/wangshao1)

## Improvement

- 更新 Pika Docker Readme, 可以按照 Readme 在 Docker 中部署 Pika 服务[#2743](https://github.com/OpenAtomFoundation/pika/pull/2743)@[luky116](https://github.com/luky116)

- 优化重复查询 meta value 导致影响 Pika 服务读写性能的问题[#2735](https://github.com/OpenAtomFoundation/pika/pull/2735)@[wangshao1](https://github.com/wangshao1)

- 支持对更多的 RocksDB 参数进行动态调整，用户根据不同的业务使用场景调整参数提升 Pika 的读写性能[#2728](https://github.com/OpenAtomFoundation/pika/pull/2728)@[cheniujh](https://github.com/cheniujh)

- 对 HyperLogLog 和 String 进行类型隔离，确保 HyperLogLog 操作与 String 操作明确区分开[#2720](https://github.com/OpenAtomFoundation/pika/pull/2720)@[saz97](https://github.com/saz97)

- 更新了 PR 标题验证，不允许在标题末尾出现中文字符[#2718](https://github.com/OpenAtomFoundation/pika/pull/2718)@[baerwang](https://github.com/baerwang)

- 重构主从复制模式 slave 节点的主从同步线程模型，尽可能减少 binlog 消费阻塞问题[#2638](https://github.com/OpenAtomFoundation/pika/pull/2638)@[cheniujh](https://github.com/cheniujh)

- 新增 RocksDB Compaction 策略动态调整参数，用户可以根据业务调整 Compaction 策略，降低  Compaction 操作对服务性能的损耗[#2538](https://github.com/OpenAtomFoundation/pika/pull/2538)@[MalikHou](https://github.com/MalikHou)

## Bugfix

- 修复 iter 未被析构，导致 pkpatternmatchdel 在返回之前不会删除 iter，这可能会导致 rocksdb 永远引用一个版本，导致数据不符合预期的问题[#2785](https://github.com/OpenAtomFoundation/pika/pull/2785)@[wangshao1](https://github.com/wangshao1)

- 修复 config 参数 min-blob-size 带单位时解析错误的问题[#2767](https://github.com/OpenAtomFoundation/pika/pull/2767)@[wangshao1](https://github.com/wangshao1)

- 修复 zverank 返回值异常的问题[#2763](https://github.com/OpenAtomFoundation/pika/pull/2763)@[chejinge](https://github.com/chejinge)

- 修复 Pika-port 传输数据过程中报错的问题[#2758](https://github.com/OpenAtomFoundation/pika/pull/2758)@[guangkun123](https://github.com/guangkun123)

- 修复因为堆上分配的缓冲区越界导致 Dbsize 命令运行时崩溃的问题 [#2749](https://github.com/OpenAtomFoundation/pika/pull/2749)@[wangshao1](https://github.com/wangshao1)

- 修复批量扩容时，多个 slave 同时连接 master, 短时间多次 bgsave 导致部分从节点数据不完整的问题[#2746](https://github.com/OpenAtomFoundation/pika/pull/2746)@[cheniujh](https://github.com/cheniujh)

- 修复参数未初始化导致 slotsscan 等命令不能和 bgsave 命令相互制衡的问题[#2745](https://github.com/OpenAtomFoundation/pika/pull/2745)@[chejinge](https://github.com/chejinge)

- 修复 Slotmigrate 迁移数据的过程中，返回值设置错误，异常场景下会终止数据迁移的问题[#2741](https://github.com/OpenAtomFoundation/pika/pull/2741)@[chejinge](https://github.com/chejinge)

- 修复 Mget 没有使用解析 ttl 的函数导致出现部分key的ttl未被更新，数据不一致的问题[#2730](https://github.com/OpenAtomFoundation/pika/pull/2730)@[chejinge](https://github.com/chejinge)

- 修复 pkpatternmatchdel 命令使用错误导致的 stream 类型数据删除异常的问题[#2726](https://github.com/OpenAtomFoundation/pika/pull/2726)@[wangshao1](https://github.com/wangshao1)

- 修复 pkpatternmatchdel 不能正确删除掉对应的 keys 的问题[#2717](https://github.com/OpenAtomFoundation/pika/pull/2717)@[wangshao1](https://github.com/wangshao1)

- 修复 ACL 密码验证错误问题[#2714](https://github.com/OpenAtomFoundation/pika/pull/2714)@[luky116](https://github.com/luky116)

- 修复 Keyspace 命令未计算 Stream 类型数据的问题[#2705](https://github.com/OpenAtomFoundation/pika/pull/2705)@[wangshao1](https://github.com/wangshao1)

- 对部分命令定制化处理逻辑，避免写 binlog 导致从节点的 binlog 解析失败的问题[#2693](https://github.com/OpenAtomFoundation/pika/pull/2693)@[cheniujh ](https://github.com/cheniujh)

- 修复 Pika cmdID 赋值在 Cmd 初始函数中，可能会导致并发构造的时候出现内存泄漏的问题[#2692](https://github.com/OpenAtomFoundation/pika/pull/2692)@[gukj-spel](https://github.com/gukj-spel)

- 修复 ExpectedStale 未考虑 String 类型, 如果存在已经过期的 String 类型的 key, ExpectedStale 会返回错误的问题[#2682](https://github.com/OpenAtomFoundation/pika/pull/2682)@[wangshao1](https://github.com/wangshao1)

- 修复 Spop 在写 binlog 时可能会出现竞态问题[#2674](https://github.com/OpenAtomFoundation/pika/pull/2674)@[cheniujh](https://github.com/cheniujh)

- db instance 设置不合理时，给用户错误提示[#2672](https://github.com/OpenAtomFoundation/pika/pull/2672)@[Mixficsol](https://github.com/Mixficsol)

- 修复 server_stat 中的数据竞态问题[#2671](https://github.com/OpenAtomFoundation/pika/pull/2671)@[cheniujh](https://github.com/cheniujh)

- 修复多 DB 下全量同步超时后不重试的问题[#2667](https://github.com/OpenAtomFoundation/pika/pull/2667)@[cheniujh](https://github.com/cheniujh)

- 修复多 DB 主从超时场景下，可能会出现窗口崩溃的问题[#2666](https://github.com/OpenAtomFoundation/pika/pull/2666)@[cheniujh](https://github.com/cheniujh)

- 修复主从同步限速逻辑中重复解锁的问题[#2657](https://github.com/OpenAtomFoundation/pika/pull/2657)@[cheniujh](https://github.com/cheniujh)

- 发版支持自动打包 centos7 和 centos8 平台的二进制编译包[#2535](https://github.com/OpenAtomFoundation/pika/pull/2535)@[baerwang](https://github.com/baerwang)

- 修复 Codis 侧的 getrange 命令没有返回预期结果的问题[#2510](https://github.com/OpenAtomFoundation/pika/pull/2510)@[luky116](https://github.com/luky116)


# v3.5.4

## New features

- Pika 支持动态调整全量同步限速参数 rsync-timeout-ms 和 throttle-bytes-per-second[#2633](https://github.com/OpenAtomFoundation/pika/pull/2633)@[cheniujh](https://github.com/cheniujh)

- 将 info key space 1 的结果输出至 info all 并展示到监控界面中[#2603](https://github.com/OpenAtomFoundation/pika/pull/2603)@[XiaoLiang2333](https://github.com/XiaoLiang2333)

- Pika 磁盘IO 限速参数支持 OnlyRead、OnlyWrite、ReadAndWrite，默认支持OnlyWrite[#2599](https://github.com/OpenAtomFoundation/pika/pull/2599)@[vacheli](https://github.com/vacheli)

## Improvement

- slotmigrate 添加 go test [#2576](https://github.com/OpenAtomFoundation/pika/pull/2576)@[chejinge](https://github.com/chejinge)

- INFO 命令耗时优化，降低查磁盘频率 [#2554](https://github.com/OpenAtomFoundation/pika/pull/2554) @[wangshao1](https://github.com/wangshao1)

- 对五种基本数据类型命令增加 Redis tcl 测试 [#2527](https://github.com/OpenAtomFoundation/pika/pull/2527)@[Mixficsol](https://github.com/Mixficsol)

## Bugfix

- 修复使用 `Pika Exporter` 时可能会出现 slots 分配不均衡的问题[#2651](https://github.com/OpenAtomFoundation/pika/pull/2651)@[chejinge](https://github.com/chejinge)

- 修复 Codis dashboard 不能正确更新 master 实例状态的问题[#2650](https://github.com/OpenAtomFoundation/pika/pull/2650)@[vacheli](https://github.com/vacheli)

- 修复 Redis 事务 binlog 解析失败导致的主从同步异常问题[#2642](https://github.com/OpenAtomFoundation/pika/pull/2642)@[chejinge](https://github.com/chejinge)

- 修复 `Pika Expoter` 启动时不带参数导致启动失败问题[#2640](https://github.com/OpenAtomFoundation/pika/pull/2640)@[Polaris3003](https://github.com/Polaris3003)

- 修复使用 `Pika Operater` 拉起集群`Codis-proxy` panic 的问题  [#2632](https://github.com/OpenAtomFoundation/pika/pull/2633)@[chejinge](https://github.com/chejinge)

- 修复 CI 编译出的二进制进行自动化测试时 cp 命令失败问题[#2614](https://github.com/OpenAtomFoundation/pika/pull/2614)@[cheniujh](https://github.com/cheniujh)

- 修复变量未初始化导致 cache 启动失败的问题[#2613](https://github.com/OpenAtomFoundation/pika/pull/2613)@[chejinge](https://github.com/chejinge)

- 修复 userpass 和 userblacklist 动态修改参数功能异常问题[#2600](https://github.com/OpenAtomFoundation/pika/pull/2600)@[chejinge](https://github.com/chejinge)

- 修复 scard sscan 结果不一致的问题[#2596](https://github.com/OpenAtomFoundation/pika/pull/2596)@[chejinge](https://github.com/chejinge)

- 修复当 max-rsync-parallel-num 大于4，slave 会在主从复制时 coredump 的问题[#2595](https://github.com/OpenAtomFoundation/pika/pull/2595)@[chejinge](https://github.com/chejinge)

- 调整不常用的线程池线程数，避免因为空跑导致性能损耗[#2590](https://github.com/OpenAtomFoundation/pika/pull/2590) @[chejinge](https://github.com/chejinge)

- 修复 Pika 事务边缘测试 case 不通过的问题[#2586](https://github.com/OpenAtomFoundation/pika/pull/2586) @[chejinge](https://github.com/chejinge)

- 将 cache-model 修改成 cache-mode[#2585](https://github.com/OpenAtomFoundation/pika/pull/2585)@[chejinge](https://github.com/chejinge)

- 修复使用 `info keyspace` 后，`info all` 死锁的问题[#2584](https://github.com/OpenAtomFoundation/pika/pull/2584) @[chejinge](https://github.com/chejinge)

- 修复因修改 zsetscorekeycomparatorimpl 字典序比较熟顺序，导致353 352极端场景不兼容的问题[#2583](https://github.com/OpenAtomFoundation/pika/pull/2583) @[wangshao1](https://github.com/wangshao1)

- 修复 `compact` 死锁的问题 [#2581](https://github.com/OpenAtomFoundation/pika/pull/2581) @[chejinge](https://github.com/chejinge)

- slotmigrate 添加 go test [#2576](https://github.com/OpenAtomFoundation/pika/pull/2576)@[chejinge](https://github.com/chejinge)

- 更新 `Pika Operater` 使用的 pika 版本[#2572](https://github.com/OpenAtomFoundation/pika/pull/2572)@[chejinge](https://github.com/chejinge)

- 修复 config rewrite 后 blockcache 数值异常的问题[#2561](https://github.com/OpenAtomFoundation/pika/pull/2561)@[chejinge](https://github.com/chejinge)

- 修复 slotmigrate 动态修复后值错误的问题[#2548](https://github.com/OpenAtomFoundation/pika/pull/2548)@[chejinge](https://github.com/chejinge)

- 修复 spop 可能会出现主从数据不一致的问题[#2541](https://github.com/OpenAtomFoundation/pika/pull/2541)@[chenbt-hz](https://github.com/chenbt-hz)

- 修复 CloseFd(it->second[i]) 出现越界的问题[#2539](https://github.com/OpenAtomFoundation/pika/pull/2539)@[chejinge](https://github.com/chejinge)

- 修复 Flushall 和 FlushDB 死锁的隐患，并删除 FlushSubDB 接口[#2533](https://github.com/OpenAtomFoundation/pika/pull/2533)@[Mixficsol](https://github.com/Mixficsol)

- 增加参数控制是否清理 tcl 测试后产生的数据文件，防止废弃数据占据磁盘[#2507](https://github.com/OpenAtomFoundation/pika/pull/2507)@[Mixficsol](https://github.com/Mixficsol)


# v3.5.3

## 新特性

- Pika 支持 ACL[#2013](https://github.com/OpenAtomFoundation/pika/pull/2013) @[lqxhub](https://github.com/lqxhub)

- 在 Codis dashboard 协程 panic 时自动恢复服务[#2349](https://github.com/OpenAtomFoundation/pika/pull/2349)@[chengyu-l](https://github.com/chengyu-l)

- 在全量复制的过程中，pika 服务的从节点不接收读流量请求 [#2197](https://github.com/OpenAtomFoundation/pika/pull/2197)  @[tedli](https://github.com/tedli)

- Pika cache 新增 bimap 数据类型[#2253](https://github.com/OpenAtomFoundation/pika/pull/2253)  @[chejinge](https://github.com/chejinge)

- 删除 Sharing 模式残留的 Slot，Pika 下只有 DB，一个 Pika 下有多个 DB[#2251](https://github.com/OpenAtomFoundation/pika/pull/2251) @[Mixficsol](https://github.com/Mixficsol)

- Pika exporter 暴露 cache 相关的数据采集指标[#2318](https://github.com/OpenAtomFoundation/pika/pull/2318) @[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- Pika 支持快慢命令分离[#2162](https://github.com/OpenAtomFoundation/pika/pull/2162) @[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- pika 执行完成 Bgsave后, 保留 unix timepoint[#2167](https://github.com/OpenAtomFoundation/pika/pull/2167) @[hero-heng](https://github.com/hero-heng)

- Pika 支持动态配置 disable_auto_compations 参数[#2257](https://github.com/OpenAtomFoundation/pika/pull/2257) @[hero-heng](https://github.com/hero-heng)

- Pika 支持 Redis Stream[#1955](https://github.com/OpenAtomFoundation/pika/pull/1955) @[KKorpse](https://github.com/KKorpse)

- Pika 支持大 key 分析工具[#2195](https://github.com/OpenAtomFoundation/pika/pull/2195) @[sjcsjc123](https://github.com/sjcsjc123)

- Pika 支持动态调整 Pika cache 参数[#2253](https://github.com/OpenAtomFoundation/pika/pull/2253) @[chejinge](https://github.com/chejinge)

- 更新 Pika benchmark 工具支持更多的接口压测[#2222](https://github.com/OpenAtomFoundation/pika/pull/2222)@[wangshao1](https://github.com/wangshao1)

- Pika Operator 支持 pika 集群自动扩容[#2121](https://github.com/OpenAtomFoundation/pika/pull/2121)@[machinly](https://github.com/machinly/)

- 添加 CompactRange 命令支持对一定范围内的 key 进行 compact[#2163](https://github.com/OpenAtomFoundation/pika/pull/2163)@[u6th9d](https://github.com/u6th9d)

- 提升 Compaction 速度减少 Compaction 耗时[#2172](https://github.com/OpenAtomFoundation/pika/pull/2172)@[u6th9d](https://github.com/u6th9d)

- 升级 RocksDB 版本到 v8.7.3[#2157](https://github.com/OpenAtomFoundation/pika/pull/2157)@[JasirVoriya](https://github.com/JasirVoriya)

- Pika 分布式集群 Codis proxy 新增可观测指标[#2199](https://github.com/OpenAtomFoundation/pika/pull/2199)@[dingxiaoshuai](https://github.com/dingxiaoshuai123)

- Pika 分布式集群支持自动 failover[#2386](https://github.com/OpenAtomFoundation/pika/pull/2386)@[chengyu-l](https://github.com/chengyu-l)

- Pika 支持 redis rename-command 功能[#2455](https://github.com/OpenAtomFoundation/pika/pull/2455)@[Mixficsol](https://github.com/Mixficsol)

- 优化 codis slot 迁移速度，支持动态修改迁移线程和速度[#2486](https://github.com/OpenAtomFoundation/pika/pull/2486) @[chejinge](https://github.com/chejinge)

- Pika 支持动态调整 max-conn-rbuf-size 参数[#2434](https://github.com/OpenAtomFoundation/pika/pull/2434) @[HappyUncle](https://github.com/HappyUncle)

- Pika-operator 支持 namespace，可以在不同的 namespace 下部署不同的集群[#2480](https://github.com/OpenAtomFoundation/pika/pull/2480) @[Y-Rookie](https://github.com/Y-Rookie)

- Pika-operator 支持监控指标采集，自动拉起 Pika-expoter[#2451](https://github.com/OpenAtomFoundation/pika/pull/2451) @[chengyu-l](https://github.com/chengyu-l)

- ACL 向前兼容 userblacklist[#2459](https://github.com/OpenAtomFoundation/pika/pull/2459) @[dingxiaoshuai](https://github.com/dingxiaoshuail)

- 丰富了 Pika TCL 测试集[#2497](https://github.com/OpenAtomFoundation/pika/pull/2497) @[Mixficsol](https://github.com/Mixficsol)

- 丰富了 Pika Gotest 测试集[#2502](https://github.com/OpenAtomFoundation/pika/pull/2502) @[Mixficsol](https://github.com/Mixficsol)

## Bugfix

- 修复 Pika 有从节点进行全量复制期间会误删除 dump 文件的问题[#2377](https://github.com/OpenAtomFoundation/pika/pull/2377)@[wangshao1](https://github.com/wangshao1)

- 修复主从复制过程中, slave 节点收到 master 异常回包后的处理逻辑[#2319](https://github.com/OpenAtomFoundation/pika/pull/2319)@[wangshao1](https://github.com/wangshao1)

- 在 Pika 执行 shutdown 命令时调用 disable compaction, 提升进程退出速度 [#2345](https://github.com/OpenAtomFoundation/pika/pull/2345) @[panlei-coder](https://github.com/panlei-coder)

- 修复 Codis-dashboard Redis Memory 值不准确的问题[#2337](https://github.com/OpenAtomFoundation/pika/pull/2337) @[Mixficsol](https://github.com/Mixficsol)

- INFO 命令耗时优化，降低查磁盘频率 [#2197](https://github.com/OpenAtomFoundation/pika/pull/2197)  @[chejinge](https://github.com/chejinge)

- 修复 Rsync 删除临时文件路径不对，删除失败，导致rocksdb打开失败的问题[#2186](https://github.com/OpenAtomFoundation/pika/pull/2186)@[wangshao1](https://github.com/wangshao1)

- 修复 Compact ,Bgsave ,Info keyspace 命令未指定db名称，导致部分命令 coredump 的问题[#2194](https://github.com/OpenAtomFoundation/pika/pull/2194)@[u6th9d](https://github.com/u6th9d)

- Codis dashboard 用 info replication 替代 info 命令查寻 master ip 降低对 Pika 的性能影响 [#2198](https://github.com/OpenAtomFoundation/pika/pull/2198)  @[chenbt-hz](https://github.com/chenbt-hz)

- 修复 Pika cache 使用边缘case，解决部分场景下 cache 和 DB 数据不一致的问题[#2225](https://github.com/OpenAtomFoundation/pika/pull/2225)  @[chejinge](https://github.com/chejinge)

- 修复当 dump 文件夹为空时，会启动报错 Segmentation fault 的问题[#2265](https://github.com/OpenAtomFoundation/pika/pull/2265)  @[chenbt-hz](https://github.com/chenbt-hz)

- 修复因为flag计算错误,导致的部分命令缓存没有生效问题[#2217](https://github.com/OpenAtomFoundation/pika/pull/2217) @[lqxhub](https://github.com/lqxhub)

- 修复主从复制模式下，主实例 flushdb 后，从实例因为死锁导致的不能访问的问题[#2249](https://github.com/OpenAtomFoundation/pika/pull/2249)@[ForestLH](https://github.com/ForestLH)

- 修复部分命令未对 RocksDB 的返回值进行判断的问题[#2187](https://github.com/OpenAtomFoundation/pika/pull/2187)@[callme-taota](https://github.com/callme-taota)

- 规范函数的返回值及初始值[#2176](https://github.com/OpenAtomFoundation/pika/pull/2176)@[Mixficsol](https://github.com/Mixficsol)

- 修复网络监控指标统计不准确的问题[#2234](https://github.com/OpenAtomFoundation/pika/pull/2234)@[chengyu-l](https://github.com/chengyu-l)

- 修复配置文件加载部分参数异常的问题[#2218](https://github.com/OpenAtomFoundation/pika/pull/2218)@[jettcc](https://github.com/jettcc)

- 修复 Codis dashboard cpu 100% 的问题[#2393](https://github.com/OpenAtomFoundation/pika/pull/2393)@[chengyu-l](https://github.com/chengyu-l)

- 修复 Codis fe pika 主从角色显示异常的问题[#2387](https://github.com/OpenAtomFoundation/pika/pull/2387)@[chengyu-l](https://github.com/chengyu-l)

- 修复迁移数据后数据不一致的问题[#2485](https://github.com/OpenAtomFoundation/pika/pull/2485)@[chejinge](https://github.com/chejinge)

- 修复dbsize 计算错误问题[#2494](https://github.com/OpenAtomFoundation/pika/pull/2494)@[chejinge](https://github.com/chejinge)

- 修复扩缩容或者 pod 起停后，Codis-dashboard 界面显示不准确的问题[#2475](https://github.com/OpenAtomFoundation/pika/pull/2475)@[chengyu-l](https://github.com/chengyu-l)

- 修复 DB 层重复上锁的问题[#2372](https://github.com/OpenAtomFoundation/pika/pull/2372) @[Mixficsol](https://github.com/Mixficsol)

- 修复全量复制失败后，未做处理导致数据丢失问题[#2439](https://github.com/OpenAtomFoundation/pika/pull/2439)@[wangshao1](https://github.com/wangshao1)

- 修复主从复制过程中，主实例执行 bgsave 过程中，没有正确回应从的同步请求[#2437](https://github.com/OpenAtomFoundation/pika/pull/2437)@[wangshao1](https://github.com/wangshao1)

- 全量复制过程中，添加数据同步状态，明确数据同步进度[#2430](https://github.com/OpenAtomFoundation/pika/pull/2430)@[baixin01](https://github.com/baixin01)

- 修复从库在 Apply binlog 时，没有对操作的 key 加锁，导致数据不一致的问题[#2409](https://github.com/OpenAtomFoundation/pika/pull/2490) @[chejinge](https://github.com/chejinge)

- 修复 codis slot 迁移过程中 master 实例 coredump 的问题[#2415](https://github.com/OpenAtomFoundation/pika/pull/2415) @[chejinge](https://github.com/chejinge)

- 修复在主从复制过程中，删除正在使用的 dump 文件的问题[#2377](https://github.com/OpenAtomFoundation/pika/pull/2377)@[wangshao1](https://github.com/wangshao1)

- 修复主从复制过程中从实例 rsync 响应错误的问题[#2319](https://github.com/OpenAtomFoundation/pika/pull/2319)@[wangshao1](https://github.com/wangshao1)

- 修复主从复制模式下，主实例flushdb后，从实例因死锁而无法访问的问题[#2372](https://github.com/OpenAtomFoundation/pika/pull/2372) @[Mixficsol](https://github.com/Mixficsol)

# v3.5.2

## 新特性

- Pika 支持 Redis 事务 [#2124](https://github.com/OpenAtomFoundation/pika/pull/2124) @[ForestLH](https://github.com/ForestLH)

- Pika 上层增加缓存层实现冷热数据分离，提升读性能 [#2133](https://github.com/OpenAtomFoundation/pika/pull/2133) @[chejinge](https://github.com/chejinge)@[Mixficsol](https://github.com/Mixficsol)

- Codis-Proxy 支持动态修改配置参数 [#2103](https://github.com/OpenAtomFoundation/pika/pull/2103) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- 补全 Go Test 测试用例 [#2063](https://github.com/OpenAtomFoundation/pika/pull/2063)@[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- CI 增加 cache 提升编译速度 [#2088](https://github.com/OpenAtomFoundation/pika/pull/2088)@[baerwang](https://github.com/baerwang)

## Bugfix

- 修复 pika 在使用 SETRANGE 命令出现 coredump的问题 [#2125](https://github.com/OpenAtomFoundation/pika/issues/2125) @[chejinge](https://github.com/chejinge)

- 修复因删除 Clearreplicationid 写进 binlog 导致的全量复制问题 [#2136](https://github.com/OpenAtomFoundation/pika/issues/2136) @[Mixficsol](https://github.com/Mixficsol)

- 修改锁粒度，提升 pika 写 binlog 的性能 [#2129](https://github.com/OpenAtomFoundation/pika/pull/2129) @[wangshao1](https://github.com/wangshao1)

- 增加 redis-copy 流量复制工具 [#2044](https://github.com/OpenAtomFoundation/pika/issues/2044)  @[Mixficsol](https://github.com/Mixficsol)

- 修复复杂数据类型成员变量可能出现数据溢出  [#2016](https://github.com/OpenAtomFoundation/pika/pull/2106)  @[u6th9d](https://github.com/u6th9d)

- 修复 decr 命令返回值错误问题 [#2092](https://github.com/OpenAtomFoundation/pika/pull/2092) @[dingxiaoshuai123](https://github.com/dingxiaoshuai123)

- 修复 setrange 和 setbit 命令没有保留原 key 的过期时间的问题 [#2095](https://github.com/OpenAtomFoundation/pika/pull/2095) @[u6th9d](https://github.com/u6th9d)


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

## Bugfix

- 减少了 exporter 非必要的日志打印，降低 CPU 利用率 [#1945](https://github.com/OpenAtomFoundation/pika/pull/1945) @[Mixficsol](https://github.com/Mixficsol)

- 升级了 sentinel 机制，对于掉线的主节点，进行逻辑删除 [#1949](https://github.com/OpenAtomFoundation/pika/pull/1949) @[Mixficsol](https://github.com/Mixficsol)

- 调整 rate_limit 参数，修复压测时出现 RPS 为 0 的情况 [#2009](https://github.com/OpenAtomFoundation/pika/pull/2009) @[chejinge](https://github.com/chejinge)

- 修复了 INFODATA 命令中对于遍历数据文件时出现空路径的逻辑判断 [#1996](https://github.com/OpenAtomFoundation/pika/pull/1996) @[Mixficsol](https://github.com/Mixficsol)

- 完善了文档的一些部分缺失的描述 [#1962](https://github.com/OpenAtomFoundation/pika/pull/1962) @[baerwang](https://github.com/baerwang)

- 使用 make -j 提高构建速度 [#1933](https://github.com/OpenAtomFoundation/pika/pull/1933) @[xiezheng-XD](https://github.com/xiezheng-XD)

- 解决了 Codis 在线上出现大毛刺的问题 [#2016](https://github.com/OpenAtomFoundation/pika/pull/2016) @[chejinge](https://github.com/chejinge)

- 修复了 Macos 环境下编译使用 tools 导致编译不过的问题 [#2011](https://github.com/OpenAtomFoundation/pika/pull/2011) @[A2ureStone](https://github.com/A2ureStone)


# v3.5.0

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
