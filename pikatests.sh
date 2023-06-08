#!/bin/bash

# clear the log file
function cleanup() {
    rm -rf ./log
    rm -rf db
    rm -rf dbsync/
    rm src/redis-server
}

# check if tcl is installed
function check_tcl {
    if [ -z "$(which tclsh)" ]; then
        echo "tclsh is not installed"
        exit 1
    fi
}

# handle different build directories.
function setup_build_dir {
    BUILD_DIR=""
    if [ -d "./build" ] && [ -d "./output" ]; then
      echo "Both build and output directories exist. Please remove one of them."
      exit 1
    fi
    if [ -d "./build" ]; then
      BUILD_DIR="build"
    fi

    if [ -d "./output" ]; then
      BUILD_DIR="output"
    fi
    if [ -z "$BUILD_DIR" ]; then
      echo "No build directory found. Please build the project first."
      exit 1
    fi
    echo "BUILD_DIR: $BUILD_DIR"
}

# setup pika bin and conf
function setup_pika_bin {
    PIKA_BIN="./$BUILD_DIR/pika"
    if [ ! -f "$PIKA_BIN" ]; then
        echo "pika bin not found"
        exit 1
    fi
    cp $PIKA_BIN src/redis-server
    cp conf/pika.conf tests/assets/default.conf
}


cleanup

check_tcl

setup_build_dir

setup_pika_bin

echo "run pika tests $1"

if [ "$1" == "all" ]; then
    tclsh tests/test_helper.tcl --clients 1
else
    tclsh tests/test_helper.tcl --clients 1 --single unit/$1
fi

if [ $? -ne 0 ]; then
    echo "pika tests failed"
    exit 1
fi
cleanup
