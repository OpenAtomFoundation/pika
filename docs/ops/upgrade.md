
# 如何升级到Pika3.0


## 升级准备工作:  
pika在2.3.3版本时为了确保同步的可靠性,增加了server-id验证功能,因此pika2.3.3~pika2.3.6与pika2.3.3之前的版本无法互相同步  
* 如果你的pika版本<2.3.3, 你需要准备pika2.3.6及pika3.0.16的bin文件，这里需要注意的是3.0.x需要准备3.0.16以后的版本（或者3.0.6版本），其他版本pika不再能与低版本（2.3.X）进行同步，因此在升级时建议使用最新的3.0.x版本pika来完成整个操作。下文以3.0.16为例。
* 如果你的pika版本>=2.3.3, 你需要准备pika3.0.16的bin文件
* 如果你的pika版本>=2.3.3那么请从下列步骤5开始, 否则请从步骤1开始
* 如果你的pika非集群模式(单点), 且无法停机升级, 请在操作前为该pika补充一个从库

## 升级步骤:
1. 为pika 从库的配置文件增加masterauth参数, 注意, 该参数的值需要与主库的requirepass参数配置相同, 否则会造成验证失败
2. 使用新的(建议使用2.3.6)pika bin目录覆盖低版本目录
3. 分别关闭pika主,从库并使用新的pika bin文件启动
4. 登录从库恢复主从关系(若你的从库配置文件中已经配置了slaveof参数, 那么可忽略该步骤), 并观察同步状态是否为up
5. 将3.0.16版本的pika部署到本服务器
6. 登录从库执行bgsave, 此时你可以在从库的dump目录中获取一份全新的备份, 确保其中的info文件在之后的步骤中不丢失, 例如你可以将其中的信息复制一份
7. 使用pika3.0.16 tools目录中的nemo_to_blackwidow工具将读取该备份中的数据并生成与pika3.0.16新引擎匹配的数据文件, 该工具的使用方法为:  
```
./nemo_to_blackwidow nemo_db_path(需要读取的备份文件目录配置) blackwidow_db_path(需要生成的新数据文件目录配置) -n(线程数量, 请根据服务器性能酌情配置, 避免消耗过多硬件资源)

例子: 低版本pika目录为pika-demo, 版本为3.0.16的pika目录为pika-new30:
./nemo_to_blackwidow /data/pika-demo/dump/backup-20180730 /data/pika-new30/new_db -n 6
```
8. 更新配置文件, 将转换好的目录(/data/pika-new30/new_db)配置为新pika的启动目录(配置文件中的db-path参数), 同时将identify-binlog-type参数配置为old, 确保3.0.16能够解析低版本pika的同步数据, 如果配置文件中存在slaveof信息请注释, 其余配置不变
9. 关闭本机从库(/data/pika-demo), 使用pika3.0.16的bin文件启动新的pika(/data/pika-new30/new_db)
10. 登录pika3.0.16, 并与主库建立同步关系, 此时请打开之前保存的备份目录中的info文件, 该文件的第4,5行分别为pika备份时同步的位置信息, 需要将该信息加入slaveof命令进行增量同步(在执行命令之前要确保主库对应的binlog文件还存在)
```
例子: info文件中的信息如下并假设主库ip为192.168.1.1端口为6666:
3s
192.168.1.2
6666
300
17055479
那么slaveof命令应为:
slaveof 192.168.1.1 6666 300 17055479
```
11. 观察同步状态是否为up, 确认同步正常后等待从库同步无延迟时该从库彻底升级完成, 此时可开始切主操作:
```
a.关闭从库的salve-read-only参数确保从库可写
b.程序端修改连接ip为从库地址, 将程序请求全部转向从库
c.在从库断开(slaveof no one)与主库的同步, 完成整个切主操作
```
12. 通过config set将identify-binlog-type配置为new并修改配置文件中该参数的值为new, 如果config set报错那么意味着你忽略了上个步骤中的c步骤
13. 此时整个升级已完成, 你获得了一个单点的pika3.0.16实例, 若需补充从库, 你可新建一个新的空的pika3.0.16或更新版本的pika实例并通同slaveof ip:port force命令来非常简单的实现主从集群的创建, 该命令能够完整同步数据后自动转换为增量同步

## 注意事项：
* 由于Pika3.0的引擎在数据存储格式上做了重新的设计，新引擎比老引擎更加节省空间，所以升级完毕后，发现Pika3.0的db要比原来小这是正常的现象
* 在数据量比较大的情况下，使用nemo_to_blackwidow工具将Nemo格式的db转换成blackwidow格式的db可能花费的时间比较久，在转换的过程中可能主库当时dump时对应的位置的binlog会被清除掉，导致最后无法进行增量同步，所以在升级之前要将原先的主库的binlog生命周期设置得长一些(修改expire-logs-days和expire-logs-nums配置项)
* 由于我们在pika3.0的版本对binlog格式做了修改，为了兼容老的版本，我们提供了identify-binlog-type选项，这个选项只有pika身份为从库的时候生效，当identify-binlog-type选项值为new的时候，表示将主库发送过来的binlog按照新版本格式进行解析(pika3.0+), 当identify-binlog-type选项值为old的时候, 表示将主库发送过来的binlog按照老版本格式进行解析(pika2.3.3 ~ pika2.3.6)
* Pika3.0对Binlog格式进行了更改，新版本的Binlog在记录更多数据的同时更加节省磁盘空间，所以在将pika2.3.6数据迁移到pika3.0过程当中判断主从同步是否无延迟时不要去对比主从各自的binlog_offset，而是看master上的Replication项slave对应的lag是否接近于0


# 如何升级到Pika3.1或3.2

# 迁移工具介绍
## manifest生成工具
* 工具路径`./tools/manifest_generator`
* 工具作用：用来进行manifest的生成
## 增量同步工具
* 工具路径`./tools/pika_port`
* 工具作用：用来进行pika3.0与新pika3.1或pika3.2之间的数据同步

# 说明
1. 为了提高pika的单机性能，自Pika3.1开始，我们在Pika中实现了redis的多库模式，正是因为如此底层存储db以及log的目录发生了一些变化，如果老版本pika的db路径是`/data/pika9221/db`，那么单db数据都会存在这个目录下面，但是由于我们目前支持了多db，目录层级比原先多了一层，所以迁移的时候我们需要手动将原先单db的数据挪到`/data/pika9221/db/db0`当中
2. 为了提高多DB的同步效率，在新版本Pika中我们使用PB协议进行实例间通信，这意味着新版本的Pika不能直接与老版本的Pika直接建立主从，所以我们需要[pika_port](https%3a%2f%2fgithub.com%2fQihoo360%2fpika%2fwiki%2fpika%e5%88%b0pika%e3%80%81redis%e8%bf%81%e7%a7%bb%e5%b7%a5%e5%85%b7)将老版本Pika的数据增量同步到新版本Pika上

# 升级步骤
1. 根据自己的场景配置新版本Pika的配置文件(databases项用于指定开启几个db)
2. 登录主库执行bgsave操作，然后将dump下来的数据拷贝到新版本pika配置文件中db-path目录的下一级目录db0中
```
例子：
    旧版本Pika dump的路径为：/data/pika_old/dump/20190517/
    新版本Pika db-path的路径为：/data/pika_new/db/
    那么我们执行： cp -r /data/pika_old/dump/20190517/ /data/pika_new/db/db0/
```
3. 使用manifest_generator工具，在新版本pika配置log目录的下一级目录log_db0中生成manifest文件，这样可以让新的pika产生和老的pika一样的binlog偏移量，需要指定db-path/db0目录和$log-path/log_db0目录（相当于把老版的db和log合并到了新版的db0里面)
```
例子：
    新版本Pika db-path的路径为： /data/pika_new/db/
    新版本Pika log-path的路径为：/data/pika_new/log/
    那么我们执行： ./manifest_generator -d /data/pika_new/db/db0 -l /data/pika_new/log/log_db0
```
4. 用v3.1.0版本的的二进制和对应的配置文件启动Pika，使用info log查看db0的binlog偏移量(filenum和offset)
5. 使用Pika-port工具将旧版Pika的数据增量同步到新版本Pika上面来
```
例子：
    旧版本Pika的ip为：192.168.1.1，端口为：9221
    新版本Pika的ip为：192.168.1.2，端口为：9222
    执行pika_port工具机器的本地ip为：192.168.1.3, 打算使用的端口为：9223
    获取的filenum为：100，获取的offset为：999

  那么我们执行：./pika_port -t 192.168.1.3 -p 9223 -i 192.168.1.1 -o 9221 -m 192.168.1.2 -n 9222 -f 100 -s 999 -e
```
6. 在使用pika-port进行同步的过程中我们可以登录主库执行`info replication`打印出的数据里会发现新增了一条从库信息，他是pika_port工具模仿slave行为与源库进行交互，同时可以通过lag查看他的延迟

7. 当我们与pika3.1或pika3.2进行增量同步的时候，可以对pika3.1或pika3.2进行从库添加操作，这样的话在从库都同步完成之后，我们在源库上查看lag如果为0 或者是很小的时候我们可以将整个集群进行替换，把原来的集群替换掉，把新集群上线


# 注意事项
1. 当我们在拷贝dump目录的时候，最好先mv改名字，然后在进行远程同步，这可以防止dump目录在拷贝的时候覆盖而造成数据不一致的结果
2. 在我们使用manifest_generator工具的时候，他需要dump时候生成的info文件，所以在拷贝dump目录的时候，要保证info文件也移动到指定的目录底下
3. 使用manifest_generator的时候 $log-path/db0 目录如果存在是会报错的，所以不要新建db0目录，脚本会自动创建
4. pika_port增量同步工具需要依赖info文件里的ip port 和file offset进行同步。
5. pika_port会模仿slave与源库交互，所以他会进行trysync，当他请求的点位在源库过期的时候，就会触发全同步，会自动的使用-r 这个参数记录的目录来存放rsync全量同步的数据，如果不想让他自动的进行全同步，可以使用-e参数，当进行全同步的时候会返回-1，但是这时候rsync还是再继续，需要kill 本地的rsync进程，源库才会终止全量同步
6. pika_port进行增量同步是持续性的，不会断的，这个时候可以在源库上使用 info replication 查看slave的lag来确定延迟
7. pika_port工具支持多线程应用，根据key hash，如果是同一个key会hash到一个线程上去回放，可以保证同一个key的顺序
8. 在数据量比较大的情况下，使用拷贝dump目录可能花费的时间比较久，导致主库当时dump时对应的位置的binlog会被清除掉（或者pika3.1(pika3.2)新增从库的时候也需要注意），导致最后无法进行增量同步，所以在升级之前要将原先的主库的binlog生命周期设置得长一些(修改expire-logs-days和expire-logs-nums配置项)
9. 如果我们不用manifest_generator 生成manifest文件也是可以的，但是这个时候启动的pika3.1或pika3.2实例的点位是 0 0 ，如果后续在pika3.1或pika3.2后挂载从库的话，需要在从库上执行`slaveof IP PORT force`命令，否则的话从库可能会出现少数据的情况
