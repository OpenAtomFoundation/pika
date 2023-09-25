#!/bin/bash

function prepare_raft_conf() {
  cp ../../output/pika ./pika

  cp ../../conf/pika.conf ./raft0.conf
  cp ../../conf/pika.conf ./raft1.conf
  cp ../../conf/pika.conf ./raft2.conf

  localip="127.0.0.1"
  sed -i -e 's|databases : 1|databases : 2|' -e 's|#daemonize : yes|daemonize : yes|' -e 's|log-path : ./log/|log-path : ./data/raft0/log/|' -e 's|db-path : ./db/|db-path : ./data/raft0/db/|' -e 's|dump-path : ./dump/|dump-path : ./data/raft0/dump/|' -e 's|pidfile : ./pika.pid|pidfile : ./data/raft0/pika.pid|' -e 's|db-sync-path : ./dbsync/|db-sync-path : ./data/raft0/dbsync/|' -e 's|is-raft : false|is-raft : true|' -e "s|# raft-cluster-endpoints : id:ip:port,id:ip:port,id:ip:port|raft-cluster-endpoints : 0:$localip:9221,1:$localip:9231,2:$localip:9241|" -e 's|raft-path : ./pikaraft/|raft-path : ./data/raft0/pikaraft/|' -e 's|snapshot-distance : 10000|snapshot-distance : 5|' -e 's|reserved-log-items : 20000|reserved-log-items : 10|' ./raft0.conf
  sed -i -e 's|databases : 1|databases : 2|' -e 's|#daemonize : yes|daemonize : yes|' -e 's|port : 9221|port : 9231|' -e 's|log-path : ./log/|log-path : ./data/raft1/log/|' -e 's|db-path : ./db/|db-path : ./data/raft1/db/|' -e 's|dump-path : ./dump/|dump-path : ./data/raft1/dump/|' -e 's|pidfile : ./pika.pid|pidfile : ./data/raft1/pika.pid|' -e 's|db-sync-path : ./dbsync/|db-sync-path : ./data/raft1/dbsync/|' -e 's|is-raft : false|is-raft : true|' -e 's|raft-server-id : 0|raft-server-id : 1|' -e "s|# raft-cluster-endpoints : id:ip:port,id:ip:port,id:ip:port|raft-cluster-endpoints : 0:$localip:9221,1:$localip:9231,2:$localip:9241|" -e 's|raft-path : ./pikaraft/|raft-path : ./data/raft1/pikaraft/|' -e 's|snapshot-distance : 10000|snapshot-distance : 5|' -e 's|reserved-log-items : 20000|reserved-log-items : 10|' ./raft1.conf
  sed -i -e 's|databases : 1|databases : 2|' -e 's|#daemonize : yes|daemonize : yes|' -e 's|port : 9221|port : 9241|' -e 's|log-path : ./log/|log-path : ./data/raft2/log/|' -e 's|db-path : ./db/|db-path : ./data/raft2/db/|' -e 's|dump-path : ./dump/|dump-path : ./data/raft2/dump/|' -e 's|pidfile : ./pika.pid|pidfile : ./data/raft2/pika.pid|' -e 's|db-sync-path : ./dbsync/|db-sync-path : ./data/raft2/dbsync/|' -e 's|is-raft : false|is-raft : true|' -e 's|raft-server-id : 0|raft-server-id : 2|' -e "s|# raft-cluster-endpoints : id:ip:port,id:ip:port,id:ip:port|raft-cluster-endpoints : 0:$localip:9221,1:$localip:9231,2:$localip:9241|" -e 's|raft-path : ./pikaraft/|raft-path : ./data/raft2/pikaraft/|' -e 's|snapshot-distance : 10000|snapshot-distance : 5|' -e 's|reserved-log-items : 20000|reserved-log-items : 10|' ./raft2.conf
}

function start_pika_raft() {
  ./pika -c ./raft$1.conf 2>/dev/null 1>/dev/null &
}

function kill_pika_raft() {
  pid1=$(ps aux | grep "./raft$1.conf" | grep -v grep | awk '{print $2}')
  kill -9 $pid1 2>/dev/null
}

function flush_pika_raft() {
  for i in {0..2}
  do
    kill_pika_raft $i
  done
  sleep 2
  rm -rf data
}

function exit_with_failure() {   
  echo "Failure at $1"
  flush_pika_raft                                                
  exit -1                                                                    
}

function prepare_test() {
  flush_pika_raft
  mkdir data data/raft0 data/raft1 data/raft2
  for i in {0..2}
  do
    start_pika_raft $i
  done
  sleep 2
}

function pass_test() {
  echo "Pass $1"  
}

function exit_with_success() {
  echo "Pass All Tests!"
  flush_pika_raft                                                   
  exit 1                                                                    
} 