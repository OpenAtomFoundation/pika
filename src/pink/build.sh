#!/bin/bash

PINK_PATH=$PWD

# We depend on pstd
PSTD_PATH=$1
if test -z $PSTD_PATH; then
  PSTD_PATH=$PINK_PATH/third/pstd
fi

if [[ ! -d $PSTD_PATH ]]; then
  mkdir -p $PSTD_PATH
  git clone https://github.com/Qihoo360/slash.git $PSTD_PATH
fi
cd $PSTD_PATH/pstd && make

# Compile pink
cd $PINK_PATH
make PSTD_PATH=$PSTD_PATH
cd examples && make PSTD_PATH=$PSTD_PATH

