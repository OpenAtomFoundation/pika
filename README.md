<img src="https://s1.ax1x.com/2020/05/08/YnbjQf.png" alt="YnbjQf.png"  width="300" />

[![Build Status](https://travis-ci.org/Qihoo360/pika.svg?branch=master)](https://travis-ci.org/Qihoo360/pika) ![Downloads](https://img.shields.io/github/downloads/Qihoo360/pika/total)

## Introduction[中文](https://github.com/Qihoo360/pika/blob/master/README_CN.md)

Pika is a persistent huge storage service , compatible  with the vast majority of redis interfaces ([details](https://github.com/Qihoo360/pika/wiki/pika-支持的redis接口及兼容情况)), including string, hash, list, zset, set and management interfaces. With the huge amount of data stored, redis may suffer for a capacity bottleneck, and pika was born for solving it. Except huge storage capacity, pika also support master-slave mode by slaveof command, including full and partial synchronization. You can also use pika together with twemproxy or codis(*pika has supported data migration in codis，thanks [left2right](https://github.com/left2right) and [fancy-rabbit](https://github.com/fancy-rabbit)*) for distributed Redis solution


## UserList

<table>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/dcHpCm4.png" alt="Qihoo"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/BIjqe9R.jpg" alt="360game"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/jjZczkN.png" alt="Weibo"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/zoel46r.gif" alt="Garena"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/kHqACbn.png" alt="Apus"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/2c57z8U.png" alt="Ffan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/rUiO5VU.png" alt="Meituan"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/px5mEuW.png" alt="XES"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://imgur.com/yJe4FP8.png" alt="HX"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/o8ZDXCH.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://imgur.com/w3qNQ9T.png" alt="GWD"></td>
<td height = "100" width = "150"><img src="https://imgur.com/KMVr3Z6.png" alt="DYD"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="http://i.imgur.com/vJbAfri.png" alt="YM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/aNxzwsY.png" alt="XM"></td>
<td height = "100" width = "150"><img src="http://i.imgur.com/mrWxwkF.png" alt="XL"></td>
<td height = "100" width = "150"><img src="http://imgur.com/0oaVKlk.png" alt="YM"></td>
</tr>
<tr>
<td height = "100" width = "150"><img src="https://i.imgur.com/PI89mec.png" alt="MM"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/G9MOvZe.jpg" alt="VIP"></td>
<td height = "100" width = "150"><img src="https://imgur.com/vQW5qr3.png" alt="LK"></td>
<td height = "100" width = "150"><img src="https://i.imgur.com/jIMG4mi.jpg" alt="KS"></td>
</tr>
</table>

[More](docs/USERS.md)

## Feature

* huge storage capacity
* compatible with redis interface, you can migrate to pika easily
* support master-slave mode (slaveof)
* various [management](https://github.com/Qihoo360/pika/wiki/pika的一些管理命令方式说明) interfaces

## For developer

### Releases
The User can download the binary release from [releases](https://github.com/Qihoo360/pika/releases) or compile the source release.

### Compile

#### Supported platforms

* linux - CentOS 6&7

* linux - Ubuntu

* macOS(Darwin) on M1

#### Dependencies

* gcc g++, C++11 support（version>=4.8）
* make
* cmake（version>=3.18）
* autoconf
* tar


#### Compile

Upgrade your gcc to version at least 4.8 to get C++11 support.

1. Get the source code

```
  git clone https://github.com/OpenAtomFoundation/pika.git
```

2. Checkout the latest release version

```
  a. exec git tag to get the latest release tag
  b. exec git checkout TAG to switch to the latest version
```

3. Compile

Please run the script build.sh before you compile this db to check the environment and build this repo.

```
  ./build.sh
```

The compilation result is in the 'output' directory.

By default the compilation process is in 'release' mode. If you wanna debug this db，you need to compile it in 'debug' mode.

```
  rm -fr output
  cmake -B output -DCMAKE_BUILD_TYPE=Debug
  cd ouput && make
```

## Usage

```
./output/pika -c ./conf/pika.conf
```

## Clean compilation

```
  If wanna clean up the compilation content, you can choose one of the following two methods as your will.
  1. exec `cd output && make clean` clean pika Compile content
  2. exec `rm -fr output` rebuild cmake（for complete recompilation）
```

## Performance

More details on [Performance](docs/benchmark/performance.md).


## Documents

1. [doc](docs/catalogue.md)


## Contact Us

Wechat: PikiwiDB (The attachment comment should be "Join Pika Wechat Group")

QQ group: 294254078

