## 1. 奇虎360
<img src="http://i.imgur.com/dcHpCm4.png" height = "50" width = "60" alt="Qihoo360">

在360, pika已替换全部redis大容量实例和ssdb实例，目前已有300+实例，每天访问量150亿，存储容量13.5T，主要业务包括手机助手、云盘等

## 2. 新浪微博
<img src="http://i.imgur.com/jjZczkN.png" height = "50" width = "60" alt="Weibo">

使用场景:
1. 上文件存储集群，有文件标识id.
2. 搜索会有一些用户属性特征pika作为存储物料库之一
3. 后台发垃圾过滤, 作为反spam

已上线

## 3. Garena
<img src="http://i.imgur.com/zoel46r.gif" height = "50" width = "60" alt="Garena">

使用场景：
1. 用在Timeline功能，读写比4:1，数据量100G多, QPS 几万
2. 电商平台推荐功能

## 4. Apus
<img src="http://i.imgur.com/kHqACbn.png" height = "50" width = "60" alt="Apus">

测试中，准备上线

## 5. 飞凡电商
<img src="http://i.imgur.com/2c57z8U.png" height = "50" width = "60" alt="Ffan">

测试中，准备上线

## 6. 美团网

<img src="http://i.imgur.com/rUiO5VU.png" height = "50" width = "60" alt="Meituan">

1. 大数据，推送业务（已上线）
2. 使用Pika 的引擎nemo 为内部的nosql 提供多数据结构接口（测试中, 准备上线）

## 7. 学而思网校
<img src="http://i.imgur.com/px5mEuW.png" height = "50" width = "60" alt="XES">

数据持久化存储（已上线）

## 8. 环信
<img src="http://imgur.com/yJe4FP8.png" height = "50" width = "60" alt="HX">

已上线

## 9. 迅雷
<img src="http://i.imgur.com/o8ZDXCH.png" height = "50" width = "60", alt="XL">

用户存储个性化推荐数据, 目前使用15台机器
已上线
