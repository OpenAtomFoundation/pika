#!/bin/bash
# This script is used by .github/workflows/pika.yml, Do not modify this file unless you know what you are doing.
# it's used to start pika, running path: build
cp ../tests/conf/pika.conf ./pika_master.conf
sed -i '' -e 's|databases : 1|databases : 2|' -e 's|#daemonize : yes|daemonize : yes|' ./pika_master.conf
./pika -c ./pika_master.conf
#ensure Pika ready
sleep 10