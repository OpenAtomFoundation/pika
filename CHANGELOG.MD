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
