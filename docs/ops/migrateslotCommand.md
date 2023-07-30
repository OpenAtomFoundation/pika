# Pika 支持 Codis sync slot 迁移设计实现

自 Pika 3.5.0 版本开始，Pika 支持 Codis + Pika 集群的模式，实现方法是将 Codis 中的 redis-server 替换成为 pika-server，增加 slot key 的概念 （默认1024个，可以通过配置文件进行配置），以 set 类型举例，在用户对每个 key 进行修改操作时，根据 slot = crc32(key) % 1024 计算出该 key 对应的 slot，然后将该 key 加到对应的 slot 中；迁移时，指定 slot，从该 slot 中 spop 出一个 key 然后迁移到目标 pika-server。该实现的特点是不需要对数据做任何操作可以直接迁移。不足之处是降低了性能（约10%～20%，所以不需要迁移数据时我们建议关闭），增加了存储（约30%），Pika 支持 slot 迁移的开关，开关关闭时，不支持 slot 迁移操作，打开开关时，支持 slot 迁移.

**conf/pika.conf 中可以设置 slotmigrate 默认为 no，在需要迁移时设置为 yes**

```bash
# slotmigrate [yes | no]
slotmigrate : no
```

**也可以通过命令行 config set 进行配置**

```bash
$ redis-cli -h 127.0.0.1 -p 9221 config set slotmigrate yes
```

## Pika 迁移 Codis 命令说明

##### slotsinfo

* 命令说明：获取 Pika 中 slot 的个数以及每个 slot 的大小

* 命令参数：slotsinfo `[start] [count]`

  1. 缺省查询 [0, MAX_SLOTNUM)
  2. start - 起始的 slot 序号
  3. count - 查询的区间的大小，即查询范围为 [start, start + count)

* 返回结果：返回 slotinfo 的 array，第一行表示 slotnum : slot 序号，第二行表示 slotsize : slot 内数据个数

  ```bash
  localhost:9221> slotsinfo 0 128
    	 1) 1) (integer) 23  
    	    2) (integer) 2
    	 2) 1) (integer) 29
            2) (integer) 1
  ```

##### slotsdel

* 命令说明：删除 Pika 中若干 slot 下的全部 key-value

* 命令参数：slotsdel slot1 [slot2 …]  接受至少 1 个 slotnum 作为参数

* 返回结果：返回删除后剩余的大小，通常为 0

  ```bash
  localhost:9221> slotsdel 1013 990
    	 1) 1) (integer) 1013
    	    2) (integer) 0
    	 2) 1) (integer) 990
    	    2) (integer) 0
  ```

##### slotsscan

* 命令说明：获取 slot key 里面的 key，使用方式同 sscan， 在 slotmigrate 为 yes 时可用

* 命令参数：参数说明类似 SCAN 命令

  1. slotnum - 查询的 slot 序号，[0, MAX_SLOT_NUM）
  2. cursor - 说明参考 SCAN 命令
  3. [COUNT count) - 说明参考 SCAN 命令

* 返回结果：参考 SCAN 命令

  ```bash
   localhost:9221> slotsscan 579 0 COUNT 10
        1) "10752"
        2)  1) "{a}7836"
            2) "{a}2167"
            3) "{a}5332"
            4) "{a}6292"
            5) "{a}600"
            6) "{a}6094"
            7) "{a}7754"
            8) "{a}4929"
            9) "{a}9211"
           10) "{a}6596"
  ```

##### slotsreload

* 命令说明：重新计算所有 key 所属的 slot

* 命令参数：不需要额外参数

  ```bash
  localhost:9221> slotsreload
       20230727234255 : 0
  ```

##### slotsreloadoff

* 命令说明：在 slotmigrate 为 yes 时可用，结束 slotsreload 命令的执行

* 命令参数：不需要额外参数

* 返回结果：返回 OK

  ```bash
  localhost:9221> slotsreloadoff
       OK
  ```

##### slotscleanup

* 命令解释：删除 slotID 对应的 key

* 命令参数：slotscleanup slotID [可以多个]

  ```bash
  localhost:9221> slotscleanup 10 11 12 13 14 15
       1) (integer) 10
       2) (integer) 11
       3) (integer) 12
       4) (integer) 13
       5) (integer) 14
       6) (integer) 15
  ```

##### slotscleanupoff

* 命令解释：停止后台执行的 slot 清除操作

* 命令参数：slotscleanupoff

* 返回结果：返回 OK

  ```bash
  localhost:9221> slotscleanupoff
       OK
  ```

## 数据迁移

##### slotsmgrtslot

* 命令说明：随机选择 slot 下的 1 个 key-value 迁移到目标机

* 命令参数：slotsmgrtslot host port timeout slot

  1. host port - 目标机
  2. timeout - 操作超时，单位 ms
  3. slot - 指定迁移的 slot 序号

* 返回结果：操作返回 int

  ```bash
  localhost:9221> set a 100            
    	 OK
  localhost:9221> slotsinfo           
    	 1) 1) (integer) 579
    	    2) (integer) 1
  localhost:9221> slotsmgrtslot 127.0.0.1 6380 100 579
         (integer) 1                      # 迁移成功
  localhost:9221> slotsinfo
    	 (empty list or set)
  localhost:9221> slotsmgrtslot 127.0.0.1 6380 100 579 1
    	 (integer) 0                     # 迁移失败，本地已经不存在了
  ```

##### slotsmgrttagslot

- 命令说明：随机选择 slot 下的 1 个 key-value 以及和该 key 相同 tag 的 key-value 迁移到目标机

- 命令参数：slotsmgrttagslot host port timeout slot

  1. host port - 目标机
  2. timeout - 操作超时，单位 ms
  3. slot - 指定迁移的 slot 序号

- 返回结果：操作返回 int

  ```bash
  localhost:9221> set a 100            
    	 OK
  localhost:9221> slotsinfo           
    	 1) 1) (integer) 579
    	    2) (integer) 1
  localhost:9221> slotsmgrttagslot 127.0.0.1 6380 100 579
         (integer) 1                      # 迁移成功
  localhost:9221> slotsinfo
    	 (empty list or set)
  localhost:9221> slotsmgrttagslot 127.0.0.1 6380 100 579 1
    	 (integer) 0                     # 迁移失败，本地已经不存在了
  ```

##### slotsmgrtone

- 命令说明：迁移 key 到目标机

- 命令参数：slotsmgrtone host port timeout key

  1. host port - 目标机
  2. timeout - 操作超时，单位 ms
  3. key 指定key

- 返回结果： 操作返回 int

  ```bash
  localhost:9221> set a{tag} 100          
       OK
  localhost:9221> slotsinfo
       1) 1) (integer) 579
          2) (integer) 1
  localhost:9221> slotsmgrtone 127.0.0.1 6380 100 a
       (integer) 1                      # 迁移成功
  localhost:9221> slotsmgrtone 127.0.0.1 6380 100 a
       (integer) 0                      # 迁移失败，本地已经不存在了
  ```

##### slotsmgrttagone

- 命令说明：迁移与 key 有相同的 tag 的 key 到目标机

- 命令参数：slotsmgrttagone host port timeout key

- 返回结果： 操作返回 int

  ```bash
  localhost:9221> set a{tag} 100        # set <a{tag}, 100>
       OK
  localhost:9221> set b{tag} 100        # set <b{tag}, 100>
       OK
  localhost:9221> slotsmgrttagone 127.0.0.1 6380 1000 {tag}
       (integer) 2
  localhost:9221> scan 0                # 迁移成功，本地不存在了
       1) "0"
       2) (empty list or set)
  localhost:6380> scan 0                # 数据一次成功迁移到目标机
       1) "0"
       2) 1) "a{tag}"
          2) "b{tag}"
  ```

**slotsmgrttagslot-async**

- 命令解释：semi-async 模式，异步将指定 slot 里指定数量的 key 迁移到目标机器
- 命令参数：slotsmgrttagslot-async hostport timeout maxbulks maxbytes slot numkeys

```bash
localhost:9221> slotsmgrttagslot-async pika 9221 5000 200 33554432 518 500
     1) (integer) 0
     2) (integer) 0
```

**slotsmgrt-exec-wrapper**

- 命令解释：semi-async 模式，处理请求的 key 属于正在迁移 slot
- 命令参数：slotsmgrt-exec-wrapper $hashkey $command [$arg1 ...]
- 返回结果：
  1. 如果该 key 不存在，则返回 0 及表示 key 不存在的信息
  2. 如果该 key 正在迁移中则返回 1 及 key 正在被迁移的信息

```bash
localhost:9221> slotsmgrt-exec-wrapper my-hash set my-hash 100
```

**slotsmgrt-async-status**

- 命令解释：semi-async 模式，查看迁移的状态
- 命令参数：slotsmgrt-async-status
- 返回结果：
  1. 目标机器
  2. slot 数量
  3. migrate 状态
  4. 移动的 key
  5. 现存的 key

```bash
localhost:9221> slotsmgrt-async-status
     1) "dest server: :-1"
     2) "slot number: -1"
     3) "migrating  : no"
     4) "moved keys : -1"
     5) "remain keys: -1"
```

**slotsmgrt-async-cancel**

- 命令解释：semi-async 模式，在 Pika 层面停止正在进行的迁移
- 命令参数：slotsmgrt-async-cancel
- 返回结果：返回 OK

```bash
localhost:9221> slotsmgrt-async-cancel
     OK
```

## 调试相关

##### slotshashkey

- 命令说明：计算并返回给定 key 的 slot 序号

- 命令参数：slotshashkey key1 [key2 …]

- 返回结果： 操作返回 array

  ```bash
  localhost:9221> slotshashkey a b c   # 计算 <a,b,c> 的 slot 序号
       1) (integer) 579                # 表示对应 key 的 slot 序号
       2) (integer) 1017
       3) (integer) 879
  ```