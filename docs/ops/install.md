## 快速试用
  如果想快速试用pika，目前提供了Centos5，Centos6和Debian(Ubuntu16) binary版本，可以在[release页面](https://github.com/Qihoo360/pika/releases)看到，具体文件是pikaX.Y.Z_xxx_bin.tar.gz。

```bash
1. unzip file
$ tar zxf pikaX.Y.Z_xxx_bin.tar.gz
2. change working directory to output
note: we should in this directory, caz the RPATH is ./lib;
$ cd output
3. run pika:
$ ./bin/pika -c conf/pika.conf
```

## 编译安装
### CentOS (Fedora, Redhat)
* 安装必要的lib

```bash
$ sudo yum install gflags-devel snappy-devel glog-devel protobuf-devel
```

* 可选择的lib
```
$ sudo yum install zlib-devel lz4-devel libzstd-devel
```

* 安装gcc

```
$ sudo yum install gcc-c++
```
* 如果机器gcc版本低于4.8，需要切换到gcc4.8或者以上，下面指令可临时切换到gcc4.8

```
$ sudo wget -O /etc/yum.repos.d/slc6-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo
$ sudo yum install --nogpgcheck devtoolset-2
$ scl enable devtoolset-2 bash
```
* 获取项目源代码

```
$ git clone https://github.com/OpenAtomFoundation/pika.git
```
* 更新依赖的子项目

```
$ cd pika
$ git submodule update --init
```

* 切换到最新release版本

```
a. 执行 git tag 查看最新的release tag，（如 v2.3.1）
b. 执行 git checkout TAG切换到最新版本，（如 git checkout v2.3.1）
```

* 编译
 
```
$ make
```

> note: 若编译过程中，提示有依赖的库没有安装，则有提示安装后再重新编译

### Debian (Ubuntu)
* 安装必要的lib

```bash
$ sudo apt-get install libgflags-dev libsnappy-dev
$ sudo apt-get install libprotobuf-dev protobuf-compiler
$ sudo apt install libgoogle-glog-dev
```


* 获取项目源代码

```bash
$ git clone https://github.com/OpenAtomFoundation/pika.git
$ cd pika
```
* 切换到最新release版本

```
a. 执行 git tag 查看最新的release tag，（如 v2.3.1）
b. 执行 git checkout TAG切换到最新版本，（如 git checkout v2.3.1）
```

* 编译
```
$ make
```

> note: 若编译过程中，提示有依赖的库没有安装，则有提示安装后再重新编译

### [静态编译方法](https://github.com/OpenAtomFoundation/pika/issues/1148)


## 使用
```
$ ./output/bin/pika -c ./conf/pika.conf
```
