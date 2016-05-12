#!/bin/bash
rm -rf PATH_TO_pika/pika_test/redis/log
rm -rf PATH_TO_pika/pika_test/redis/db
cp PATH_TO_pika/output/bin/pika PATH_TO_pika/pikatests/redis/src/redis-server
cp PATH_TO_pika/output/conf/pika.conf PATH_TO_pika/pikatests/redis/tests/assets/default.conf

tclsh tests/test_helper.tcl --clients 1 --single unit/type/$1

rm -rf PATH_TO_pika/pikatests/redis/log
rm -rf PATH_TO_pika/pikatests/redis/db
