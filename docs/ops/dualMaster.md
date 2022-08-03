# Pika双主文档
可用版本： 2.3.0 ~ 3.0.11

## 介绍
为了避免额外的维护成本, Pika在之前主从的逻辑设计上支持了双主架构, 通过`conf`文件配置双主模式. 在双主建立成功的模式下, 仍然可以通过`slaveof`为两个`master`增加`slave`节点。
## 使用方法
*  通过`pika.conf`文件配置双主模式
	
	修改双主A的`pika.conf`配置文件
	
	   	...
	   	# server-id for hub
 	   	server-id : 1
 		...
		# The peer-master config
 		double-master-ip : 	192.168.10.2 	配置另一个主的ip
 		double-master-port : 9220		配置另一个主的port
 		double-master-server-id :	2	配置另一个主的server id (注意不要与本机server id和已经连接的slave的sid重复)
 	
 	修改双主B的`pika.conf`配置文件
 	
 		...
		# server-id for hub
 		server-id : 2
 		...
 		# The peer-master config
 		double-master-ip : 	192.168.10.1	配置另一个主的ip
 		double-master-port : 9220		配置另一个主的port
 		double-master-server-id :	1	配置另一个主的server id (注意不要与本机server id和已经连接的slave的sid重复)
 		
* 分别启动两个Pika实例, 使用`info`查看信息

	查看`192.168.10.1`的`info`信息
	
		...
		# DoubleMaster(DOUBLEMASTER)
		role:double_master
		the peer-master host:192.168.10.2
		the peer-master port:9220
		double_master_mode: True
		repl_state: 3
		double_master_server_id:2
		double_master_recv_info: filenum 0 offset 0
	
	其中`repl_state`为`3`代表双主连接建立成功

## 双主启动时binlog偏移校验失败
* 双主分别启动时会检验对端实例的`binlog`偏移量, 如果两端`binlog`偏移量相差过大会造成双主连接建立失败.

	`IOFO`级别日志会报

		pika_admin.cc:169] Because the invalid filenum and offset, close the connection between the peer-masters

	利用`info`命令查看实例信息

		# DoubleMaster(MASTER)
		role:master
		the peer-master host:192.168.10.2
		the peer-master port:9220
		double_master_mode: True
		repl_state: 0
		double_master_server_id:2
		double_master_recv_info: filenum 0 offset 0
	其中`repl_state`为`0`代表双主建立失败

* 假如双主建立失败, 无需停机可以通过`slaveof`命令以某一个双主实例的为基准做数据全同步

	例如`192.168.10.2`以`192.168.10.1`的数据做全同步
		
		slaveof 192.168.10.1

## 断开双主连接
双主的两个节点分别执行`slaveof no one`用来断开连接.

## 测试场景以及测试结果

|-|场景|测试结果|备注|
|:-:|:-:|:-:|:-:|
|正确性测试|
|1|双主单边写入|测试两次，分别使用12和64线程，key数量30w-100w不等，未发现key丢失，校验value相等|通过|
|2|双主同时写入（不操作相同key）|测试3次，key数量60w左右，两边key数量相同，校验value相等|通过|
|3|双主同时写入（相同key）|测试2次，key数量40w左右，两边key数量相同，校验value相等|通过|
|4|双主同时写入（不操作相同key），使用iptables模拟网络延迟|测试3次，key数量60w左右，两边key数量相同，校验value相等|通过|
|5|双主同时写入（相同key），使用iptables模拟网络延迟|测试2次，key数量40w左右，两边key数量相同, 但部分key的value不同|通过|
|6|双主同时写入（相同key），挂载两个从库|key数量20w左右，两边key数量一致，值也相同|通过|
|运维测试|
|7|单主写入，shutdown其中一个主库，然后挂载新从库重新搭建双主|key数量100w左右，两边key数量一致，值也相同|通过|
|8|单主写入，重启其中一个主库|key数量100w左右，两边key数量一致，值也相同|通过|

## FAQ
* 双主模式下如何避免数据循环传播?
> 每个双主实例内部会维护另一个`master`的信息, 通过`binlog`中的`server id`字段来判断是否需要同步给另一个`master`

* 如何关闭双主模式?
> 双主关系可以随时取消, 只需要把其中一个节点停止即可

* 双主模式下是否支持双写?
> 双主模式下支持双写

* 双主的两个实例能否同时操作更新同一个key?
> 双主模式下操作同一个key, 不保证value的最终一致

* 双主模式下如何增加`slave`节点
> 使用正常的主从配置方法即可

* 双主A,B 因为意外情况,B实例因为宕机过久, 重启后因为`binlog`偏移相差过大双主模式建立失败, 怎么恢复?
> 以A实例为基准做全同步, 直接利用`slaveof`命令做全同步后会自动恢复双主连接
