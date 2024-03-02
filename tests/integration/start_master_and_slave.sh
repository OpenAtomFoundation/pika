#!/bin/bash
# This script is used by .github/workflows/pika.yml, Do not modify this file unless you know what you are doing.
# it's used to start pika master and slave, running path: build
cp ../../output/pika ./pika
cp ../conf/pika.conf ./pika_single.conf
cp ../conf/pika.conf ./pika_master.conf
cp ../conf/pika.conf ./pika_slave.conf
# Create folders for storing data on the primary and secondary nodes
mkdir master_data
mkdir slave_data
# Example Change the location for storing data on primary and secondary nodes in the configuration file
sed -i '' -e 's|databases : 1|databases : 2|' -e 's|#daemonize : yes|daemonize : yes|' ./pika_single.conf
sed -i '' -e 's|databases : 1|databases : 2|' -e 's|port : 9221|port : 9241|' -e 's|log-path : ./log/|log-path : ./master_data/log/|' -e 's|db-path : ./db/|db-path : ./master_data/db/|' -e 's|dump-path : ./dump/|dump-path : ./master_data/dump/|' -e 's|pidfile : ./pika.pid|pidfile : ./master_data/pika.pid|' -e 's|db-sync-path : ./dbsync/|db-sync-path : ./master_data/dbsync/|' -e 's|#daemonize : yes|daemonize : yes|' ./pika_master.conf
sed -i '' -e 's|databases : 1|databases : 2|' -e 's|port : 9221|port : 9231|' -e 's|log-path : ./log/|log-path : ./slave_data/log/|' -e 's|db-path : ./db/|db-path : ./slave_data/db/|' -e 's|dump-path : ./dump/|dump-path : ./slave_data/dump/|' -e 's|pidfile : ./pika.pid|pidfile : ./slave_data/pika.pid|' -e 's|db-sync-path : ./dbsync/|db-sync-path : ./slave_data/dbsync/|' -e 's|#daemonize : yes|daemonize : yes|' ./pika_slave.conf
# Start three nodes
./pika -c ./pika_single.conf
./pika -c ./pika_master.conf
./pika -c ./pika_slave.conf
#ensure both master and slave are ready
sleep 10
