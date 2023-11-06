#!/usr/bin/env bash
# ******************************************************
# DESC    : glog build script
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : BSD-3-Clause License
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2019-01-22 19:54
# FILE    : build.sh
# ******************************************************

mkdir -p deps
cd deps

##############
### install cmake3 && automake for compiling glog
##############

if [ ! -d "./cmake-3.13.3" ]; then
  wget https://github.com/Kitware/CMake/releases/download/v3.13.3/cmake-3.13.3.tar.gz
  tar -zxvf cmake-3.13.3.tar.gz
fi
cd cmake-3.13.3 && ./bootstrap  && gmake && sudo gmake install && cd ..
rm -f /usr/bin/cmake
ln -s /usr/local/bin/cmake /usr/bin/cmake

yum install -y automake
autoreconf -ivf

##############
### compile libunwind
##############

if [ ! -d "./libunwind-1.3.1" ]; then
  wget https://github.com/libunwind/libunwind/releases/download/v1.3.1/libunwind-1.3.1.tar.gz
  tar -xf libunwind-1.3.1.tar.gz
fi

mkdir libunwind
cd libunwind-1.3.1
./configure --prefix=`pwd`/../libunwind
make
make install
cd ..

##############
### compile gflags
##############

# 卸载系统的gflags
# sudo yum remove -y gflags-devel
# 下载编译gflags
if [ ! -d "./gflags-2.2.1" ]; then
  wget -O gflags-2.2.1.tar.gz https://github.com/gflags/gflags/archive/v2.2.1.tar.gz
  tar zxvf gflags-2.2.1.tar.gz
fi
cd gflags-2.2.1
rm -rf build && mkdir -p build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=../../gflags
make
make install
cd ../..

##############
### compile glog
##############

mkdir -p glog

# return to root dir
cd ..

cd third/glog
./configure --includedir=../../deps/gflags/include
rm -rf build && mkdir -p build && cd build
export CXXFLAGS="-fPIC" && cmake .. -DCMAKE_INSTALL_PREFIX=../../../deps/glog && make VERBOSE=1 && make install
cd ../../..

