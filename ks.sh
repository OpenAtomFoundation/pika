#!/usr/bin/env bash
# ******************************************************
# DESC    :
# AUTHOR  : Alex Stocks
# VERSION : 1.0
# LICENCE : Apache License 2.0
# EMAIL   : alexstocks@foxmail.com
# MOD     : 2023-04-22 14:28
# FILE    : ks.sh
# ******************************************************

lsof -P -i :10221 | grep -v "COMMAND" | awk '{print $2}' | xargs kill -9

rm -rf /Users/zhaoxin13/test/golang/src/github.com/AlexStocks/pika/db

