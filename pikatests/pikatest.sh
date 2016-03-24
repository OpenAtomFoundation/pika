#!/bin/bash
rm -rf /home/wuxiaofei-xy/worplace/pika/pika_test/redis/log
rm -rf /home/wuxiaofei-xy/workplace/pika/pika_test/redis/db
cp /home/wuxiaofei-xy/workplace/pika/output/bin/pika /home/wuxiaofei-xy/workplace/pika/pikatests/redis/src/redis-server
cp /home/wuxiaofei-xy/workplace/pika/output/conf/pika.conf /home/wuxiaofei-xy/workplace/pika/pikatests/redis/tests/assets/default.conf

tclsh tests/test_helper.tcl --clients 1 --single unit/type/$1

rm -rf /home/wuxiaofei-xy/workplace/pika/pikatests/redis/log
rm -rf /home/wuxiaofei-xy/workplace/pika/pikatests/redis/db
