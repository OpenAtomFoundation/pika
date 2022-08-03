#pika与ssdb性能对比

## 测试环境

相同配置服务端、客户机各1台：

    CPU: 24 Cores, Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz
    MEM: 198319652 kB
    OS: CentOS release 6.2 (Final)
    NETWORK CARD: Intel Corporation I350 Gigabit Network Connection
 
## 测试接口
set和get

## 测试方法

使用ssdb和pika默认配置(pika配16个worker)，客户机执行 ./redis-benchmark -h ... -p ... -n 1000000000 -t set,get -r 10000000000 -c 120 -d 200，通过set和get接口分别对ssdb和pika进行10亿次写入+10亿次读取

## 结果图表
<img src="images/benchmarkVsSSDB01.png" height = "400" width = "480" alt="1">
<img src="images/benchmarkVsSSDB02.png" height = "400" width = "480" alt="10">

## 详细测试结果（10亿）

### Set：

	ssdb
        1000000000 requests completed in 25098.81 seconds
		0.52% <= 1 milliseconds 
		96.39% <= 2 milliseconds 
		97.10% <= 3 milliseconds 
		97.34% <= 4 milliseconds 
		97.57% <= 5 milliseconds
		…
		98.87% <= 38 milliseconds
		...
		99.99% <= 137 milliseconds
		100.00% <= 215 milliseconds
		...
		100.00% <= 375 milliseconds
		
		39842.53 requests per second
	

	
	
		
	pika
        1000000000 requests completed in 11890.80 seconds
		18.09% <= 1 milliseconds
		93.32% <= 2 milliseconds
		99.71% <= 3 milliseconds
		99.86% <= 4 milliseconds
		99.92% <= 5 milliseconds
		99.94% <= 6 milliseconds
		99.96% <= 7 milliseconds
		99.97% <= 8 milliseconds
		99.97% <= 9 milliseconds
		99.98% <= 10 milliseconds
		99.98% <= 11 milliseconds
		99.99% <= 12 milliseconds
		...
		100.00% <= 19 milliseconds
		...
		100.00% <= 137 milliseconds
		
		84098.66 requests per second
		
### Get：

	ssdb
		1000000000 requests completed in 12744.41 seconds
        7.32% <= 1 milliseconds
		96.08% <= 2 milliseconds
		99.49% <= 3 milliseconds
		99.97% <= 4 milliseconds
		99.99% <= 5 milliseconds
		100.00% <= 6 millisecondsj
		...
		100.00% <= 229 milliseconds
		
		78465.77 requests per second
	
	
	
	pika
        1000000000 requests completed in 9063.05 seconds
		84.97% <= 1 milliseconds
		99.76% <= 2 milliseconds
		99.99% <= 3 milliseconds
		100.00% <= 4 milliseconds
		...
		100.00% <= 33 milliseconds
		
		110338.10 requests per second
		
## 测试结论

由于pika的设计支持多线程的读写（ssdb仅支持1个线程写，多个线程读），在本测试环境中，pika写性能是ssdb的2.1倍（qps：84098 vs 39842），读性能是ssdb的1.4倍（qps：110338 vs 78465）

ssdb 写延迟分布 0.52%在1ms以内, 97.10%在3ms以内, 而pika 的写延迟分布18.32%在1ms以内, 99.71%在3ms以内. 说明 pika 的写延迟在写入量相同的情况下（10亿）下明显优于ssdb

ssdb 读延迟分布7.32%在1ms以内, 99.49%在3ms以内, 而pika 的84.97%在1ms以内, 99.99%在3ms以内. 说明 pika 的读延迟在读取量相同的情况（10亿）下优于ssdb

所以在本测试环境中，pika 各个方面的性能都优于ssdb

## 注

1. ssdb的写性能开始还不错（8w\~9w)，随着写入的持续，速度逐渐降到3w左右，然后一直持续到写入结束；pika的写入速度则相对稳定，全程维持在8w\~9w
2. 本测试结果仅供参考，大家在使用pika前最好根据自己的实际使用场景来进行针对性的性能测试

		
