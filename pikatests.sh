#!/bin/bash
rm -rf ./log
rm -rf .db
cp output/pika src/redis-server
cp conf/pika.conf tests/assets/default.conf

tclsh tests/test_helper.tcl --clients 1 --single unit/$1
rm src/redis-server
rm -rf ./log
rm -rf ./db