#!/bin/bash

NET_PATH=$PWD

# We depend on pstd
PSTD_PATH=$1
if test -z $PSTD_PATH; then
  PSTD_PATH=$NET_PATH/third/pstd
fi

if [[ ! -d $PSTD_PATH ]]; then
  mkdir -p $PSTD_PATH
  git clone https://github.com/Qihoo360/slash.git $PSTD_PATH
fi
cd $PSTD_PATH/pstd && make

# Compile net
cd $NET_PATH
make PSTD_PATH=$PSTD_PATH
cd examples && make PSTD_PATH=$PSTD_PATH

