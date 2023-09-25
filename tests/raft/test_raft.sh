#!/bin/bash

source ./common.sh

port0=9221
port1=9231
port2=9241

prepare_raft_conf

#############################
#       Leader写测试
#############################
test_name="LeaderWrite"

prepare_test

value1=$(redis-cli -p $port0 set hello world)
if [ "$value1" != "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port1 get hello)
value2=$(redis-cli -p $port2 get hello)
if [ "$value1" != "$value2" ] || [ "$value1" != "world" ] || [ "$value2" != "world" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port1 get foo)
value2=$(redis-cli -p $port2 get foo)
if [ "$value1" != "$value2" ] || [ "$value1" != "" ] || [ "$value2" != "" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
#       Follower写测试
#############################
test_name="FollowerWrite"

prepare_test

value1=$(redis-cli -p $port1 set follower write)
if [ "$value1" != "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port0 get follower)
value2=$(redis-cli -p $port2 get follower)
if [ "$value1" != "$value2" ] || [ "$value1" != "write" ] || [ "$value2" != "write" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port1 get foo)
value2=$(redis-cli -p $port2 get foo)
if [ "$value1" != "$value2" ] || [ "$value1" != "" ] || [ "$value2" != "" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
#       集群不可用测试
#############################
test_name="RejectWhenUnavailable"

prepare_test

kill_pika_raft 1
kill_pika_raft 2

sleep 2

value1=$(redis-cli -p $port0 set foo bar)
if [ "$value1" == "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port0 get foo)
if [ "$value1" != "" ]; then
  exit_with_failure $test_name
fi

start_pika_raft 1 
start_pika_raft 2 

sleep 2 

value1=$(redis-cli -p $port1 get foo)
if [ "$value1" != "" ]; then
  exit_with_failure $test_name
fi
value1=$(redis-cli -p $port2 get foo)
if [ "$value1" != "" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
#       集群恢复可用测试
#############################
test_name="RecoverAvailable"

value1=$(redis-cli -p $port0 set hello pika)
if [ "$value1" != "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port1 get hello)
value2=$(redis-cli -p $port2 get hello)
if [ "$value1" != "$value2" ] || [ "$value1" != "pika" ] || [ "$value2" != "pika" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
#       落后节点同步测试
#############################
test_name="LagServer"

prepare_test

kill_pika_raft 2

sleep 2

value1=$(redis-cli -p $port0 set hello pika)
if [ "$value1" != "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port0 get hello)
value2=$(redis-cli -p $port1 get hello)
if [ "$value1" != "$value2" ] || [ "$value1" != "pika" ] || [ "$value2" != "pika" ]; then
  exit_with_failure $test_name
fi

kill_pika_raft 0
kill_pika_raft 1

sleep 2

start_pika_raft 2

sleep 2

value1=$(redis-cli -p $port2 get hello)
if [ "$value1" != "" ]; then
  exit_with_failure $test_name
fi

start_pika_raft 0

sleep 2

value1=$(redis-cli -p $port0 get hello)
value2=$(redis-cli -p $port2 get hello)
if [ "$value1" != "$value2" ] || [ "$value1" != "pika" ] || [ "$value2" != "pika" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
#       Snapshot测试
#############################
test_name="Snapshot"

prepare_test

kill_pika_raft 2

sleep 2

for i in {0..20}
do
  value1=$(redis-cli -p $port0 set snp $i)
  if [ "$value1" != "OK" ]; then
    exit_with_failure $test_name
  fi
done

kill_pika_raft 0

sleep 2

start_pika_raft 2

sleep 2

value1=$(redis-cli -p $port2 get snp)
if [ "$value1" != "20" ]; then
  exit_with_failure $test_name
fi

for i in {21..40}
do
  value1=$(redis-cli -p $port2 set snp $i)
  if [ "$value1" != "OK" ]; then
    exit_with_failure $test_name
  fi
done

kill_pika_raft 1
kill_pika_raft 2

sleep 2

start_pika_raft 0

sleep 2

value1=$(redis-cli -p $port0 get snp)
if [ "$value1" != "20" ]; then
  exit_with_failure $test_name
fi

start_pika_raft 2

sleep 2

value1=$(redis-cli -p $port0 get snp)
value2=$(redis-cli -p $port2 get snp)
if [ "$value1" != "$value2" ] || [ "$value1" != "40" ] || [ "$value2" != "40" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port2 set snp ok)
if [ "$value1" != "OK" ]; then
  exit_with_failure $test_name
fi

value1=$(redis-cli -p $port0 get snp)
value2=$(redis-cli -p $port2 get snp)
if [ "$value1" != "$value2" ] || [ "$value1" != "ok" ] || [ "$value2" != "ok" ]; then
  exit_with_failure $test_name
fi

pass_test $test_name

#############################
exit_with_success