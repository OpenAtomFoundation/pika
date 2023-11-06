#!/usr/bin/env bash
# ******************************************************
# DESC    : pika-port2 build script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : BSD-3-Clause License
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2019-01-22 19:54
# FILE    : build.sh
# ******************************************************

# https://github.com/facebook/rocksdb/blob/master/INSTALL.md

sh glog.sh

# install compression lib
sudo yum install -y snappy snappy-devel
sudo yum install -y zlib zlib-devel
sudo yum install -y bzip2 bzip2-devel
sudo yum install -y lz4-devel

# download third libs
# pls wait for about 30 minutes
git submodule update --init --recursive --force

# compile pika-port for pika 3.0.x
cd pika_port_2 && make
