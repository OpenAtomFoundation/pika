#!/bin/bash
rm -rf /home/songzhao/develop/pika_test/redis/log
rm -rf /home/songzhao/develop/pika_test/redis/db
cp /home/songzhao/develop/pika/output/bin/pika /home/songzhao/develop/pika_test/redis/src/redis-server
#cp /home/songzhao/develop/pika/output/conf/pika.conf /home/songzhao/develop/pika_test/redis/tests/assets/default.conf

tclsh tests/test_helper.tcl --clients 1 --single unit/type/$1

rm -rf /home/songzhao/develop/pika_test/redis/log
rm -rf /home/songzhao/develop/pika_test/redis/db
