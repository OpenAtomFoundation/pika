## Quick Install
  Binary version in Centos5，Centos6 and Debian(Ubuntu16) can download in [release](https://github.com/Qihoo360/pika/releases)

```bash
1. unzip file
$ tar zxf pikaX.Y.Z_xxx_bin.tar.gz
2. change working directory to output
note: we should in this directory, caz the RPATH is ./lib;
$ cd output
3. run pika:
$ ./bin/pika -c conf/pika.conf
```

## Compile Install
### CentOS (Fedora, Redhat)
* require librarys

```bash
$ sudo yum install gflags-devel snappy-devel glog-devel protobuf-devel
```

* lib
```
$ sudo yum install zlib-devel lz4-devel libzstd-devel
```

* gcc

```
$ sudo yum install gcc-c++
```
* Version of GCC >= 4.8

```
$ sudo wget -O /etc/yum.repos.d/slc6-devtoolset.repo http://linuxsoft.cern.ch/cern/devtoolset/slc6-devtoolset.repo
$ sudo yum install --nogpgcheck devtoolset-2
$ scl enable devtoolset-2 bash
```
* download source code

```
$ git clone https://github.com/OpenAtomFoundation/pika.git
```
* update sub module

```
$ cd pika
$ git submodule update --init
```

* change latest release

```
a. git tag 
b. git checkout $tag
```

* Compile
 
```
$ make
```


### Debian (Ubuntu)
* install require libs

```bash
$ sudo apt-get install libgflags-dev libsnappy-dev
$ sudo apt-get install libprotobuf-dev protobuf-compiler
$ sudo apt install libgoogle-glog-dev
```


* download source code

```bash
$ git clone https://github.com/OpenAtomFoundation/pika.git
$ cd pika
```
* change latest release

```
a. git tag 
b. git checkout $tag
```

* Compile
```
$ make
```


### [static compile](https://github.com/OpenAtomFoundation/pika/issues/1148)


## Usage
```
$ ./output/bin/pika -c ./conf/pika.conf
```

## Notice
After startup, if you find 'Attempt to free invalid pointer', can try upgrade tcmalloc.