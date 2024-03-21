#!/bin/bash
# This script is used by .github/workflows/pika.yml, Do not modify this file unless you know what you are doing.
# it's used to start pika master and slave, running path: build
cp ../../output/pika ./pika
cp ../conf/pika.conf ./pika_single.conf
cp ../conf/pika.conf ./pika_master.conf
cp ../conf/pika.conf ./pika_slave.conf
cp ../conf/pika.conf ./pika_rename.conf
cp ../conf/pika.conf ./pika_master_rename.conf
cp ../conf/pika.conf ./pika_slave_rename.conf
cp ../conf/pika.conf ./pika_acl_both_password.conf
cp ../conf/pika.conf ./pika_acl_only_admin_password.conf
cp ../conf/pika.conf ./pika_has_other_acl_user.conf
# Create folders for storing data on the primary and secondary nodes
mkdir master_data
mkdir slave_data
# Example Change the location for storing data on primary and secondary nodes in the configuration file
sed -i ''  \
  -e 's|databases : 1|databases : 2|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_single.conf

sed -i ''  \
  -e 's|databases : 1|databases : 2|'  \
  -e 's|port : 9221|port : 9241|'  \
  -e 's|log-path : ./log/|log-path : ./master_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./master_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./master_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./master_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./master_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_master.conf

sed -i ''  \
  -e 's|databases : 1|databases : 2|'  \
  -e 's|port : 9221|port : 9231|'  \
  -e 's|log-path : ./log/|log-path : ./slave_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./slave_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./slave_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./slave_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./slave_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_slave.conf

sed -i ''  \
  -e 's|# rename-command : FLUSHDB 360flushdb|rename-command : FLUSHDB 360flushdb|'  \
  -e 's|databases : 1|databases : 2|'  \
  -e 's|port : 9221|port : 9251|'  \
  -e 's|log-path : ./log/|log-path : ./rename_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./rename_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./rename_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./rename_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./rename_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_rename.conf

sed -i ''  \
  -e 's|requirepass :|requirepass : requirepass|' \
  -e 's|masterauth :|masterauth : requirepass|' \
  -e 's|# userpass :|userpass : userpass|' \
  -e 's|# userblacklist :|userblacklist : flushall,flushdb|' \
  -e 's|port : 9221|port : 9261|' \
  -e 's|log-path : ./log/|log-path : ./acl1_data/log/|' \
  -e 's|db-path : ./db/|db-path : ./acl1_data/db/|' \
  -e 's|dump-path : ./dump/|dump-path : ./acl1_data/dump/|' \
  -e 's|pidfile : ./pika.pid|pidfile : ./acl1_data/pika.pid|' \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./acl1_data/dbsync/|' \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_acl_both_password.conf

sed -i ''  \
  -e 's|requirepass :|requirepass : requirepass|'  \
  -e 's|masterauth :|masterauth : requirepass|'  \
  -e 's|# userblacklist :|userblacklist : flushall,flushdb|'  \
  -e 's|port : 9221|port : 9271|'  \
  -e 's|log-path : ./log/|log-path : ./acl2_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./acl2_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./acl2_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./acl2_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./acl2_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_acl_only_admin_password.conf

sed -i ''  \
  -e 's|requirepass :|requirepass : requirepass|'  \
  -e 's|masterauth :|masterauth : requirepass|'  \
  -e 's|# userpass :|userpass : userpass|'  \
  -e 's|# userblacklist :|userblacklist : flushall,flushdb|'  \
  -e 's|port : 9221|port : 9281|'   \
  -e 's|log-path : ./log/|log-path : ./acl3_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./acl3_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./acl3_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./acl3_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./acl3_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_has_other_acl_user.conf
echo -e '\nuser : limit on >limitpass ~* +@all &*' >> ./pika_has_other_acl_user.conf

sed -i ''  \
  -e 's|# rename-command : FLUSHDB 360flushdb|rename-command : FLUSHDB 360flushdb|'  \
  -e 's|port : 9221|port : 9291|'   \
  -e 's|log-path : ./log/|log-path : ./master_rename_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./master_rename_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./master_rename_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./master_rename_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./master_rename_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_master_rename.conf

sed -i ''  \
  -e 's|# rename-command : FLUSHDB 360flushdb|rename-command : FLUSHDB 360flushdb|'  \
  -e 's|port : 9221|port : 9301|'   \
  -e 's|log-path : ./log/|log-path : ./slave_rename_data/log/|'  \
  -e 's|db-path : ./db/|db-path : ./slave_rename_data/db/|'  \
  -e 's|dump-path : ./dump/|dump-path : ./slave_rename_data/dump/|'  \
  -e 's|pidfile : ./pika.pid|pidfile : ./slave_rename_data/pika.pid|'  \
  -e 's|db-sync-path : ./dbsync/|db-sync-path : ./slave_rename_data/dbsync/|'  \
  -e 's|#daemonize : yes|daemonize : yes|' ./pika_slave_rename.conf

# Start three nodes
./pika -c ./pika_single.conf
./pika -c ./pika_master.conf
./pika -c ./pika_slave.conf
./pika -c ./pika_rename.conf
./pika -c ./pika_acl_both_password.conf
./pika -c ./pika_acl_only_admin_password.conf
./pika -c ./pika_has_other_acl_user.conf
./pika -c ./pika_master_rename.conf
./pika -c ./pika_slave_rename.conf
#ensure both master and slave are ready
sleep 10