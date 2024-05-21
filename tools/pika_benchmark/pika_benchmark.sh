#!/bin/bash

host=127.0.0.1
port=9221
requests=10000
clients=50
threads=4
dataSize=32

usage() {
    echo "Usage: $0 [-host <host>] [-port <port>] [-requests <requests>] [-clients <clients>] [-threads <threads>] [-dataSize <dataSize>]"
    echo ""
    echo "Options:"
    echo "  -host <host>       Server hostname, default: $host"
    echo "  -port <port>       Server port, default: $port"
    echo "  -requests <requests>   Number of requests, default: $requests"
    echo "  -clients <clients>    Number of concurrent clients, default: $clients"
    echo "  -threads <threads>    Number of threads, default: $threads"
    echo "  -dataSize <dataSize>   Data size, default: $dataSize"
    echo ""
    exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -host)
      shift
      host=$1
      ;;
    -port)
      shift
      port=$1
      ;;
    -requests)
      shift
      requests=$1
      ;;
    -clients)
      shift
      clients=$1
      ;;
    -threads)
      shift
      threads=$1
      ;;
    -dataSize)
      shift
      dataSize=$1
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      ;;
  esac
  shift
done

echo "\n================================ benchmark parameters ==============================="
echo host="$host"
echo port="$port"
echo requests="$requests"
echo clients="$clients"
echo threads="$threads"
echo dataSize="$dataSize"
echo "================================================================================\n"

pwd=$(pwd)
mkdir -p $pwd/bench_data

rw_0r10w=$pwd/bench_data/rw_0r10w.txt
rw_5r5w=$pwd/bench_data/rw_5r5w.txt
rw_3r7w=$pwd/bench_data/rw_3r7w.txt
rw_7r3w=$pwd/bench_data/rw_7r3w.txt
rw_10r0w=$pwd/bench_data/rw_10r0w.txt

cmd_get=$pwd/bench_data/cmd_get.txt
cmd_set=$pwd/bench_data/cmd_set.txt

cmd_hset=$pwd/bench_data/cmd_hset.txt
cmd_hget=$pwd/bench_data/cmd_hget.txt

cmd_lpush=$pwd/bench_data/cmd_lpush.txt
cmd_rpush=$pwd/bench_data/cmd_rpush.txt
cmd_rpush=$pwd/bench_data/cmd_lrange.txt

cmd_sadd=$pwd/bench_data/cmd_sadd.txt
cmd_sadd=$pwd/bench_data/cmd_smembers.txt
cmd_spop=$pwd/bench_data/cmd_spop.txt

cmd_zadd=$pwd/bench_data/cmd_zadd.txt
cmd_zrange=$pwd/bench_data/cmd_zrange.txt

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --ratio=0:10 --select-db=0 > $rw_0r10w
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --ratio=1:1 --select-db=0 > $rw_5r5w
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --ratio=3:7 --select-db=0 > $rw_3r7w
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --ratio=7:3 --select-db=0 > $rw_7r3w
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --ratio=10:0 --select-db=0 > $rw_10r0w

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="get __key__" --command-ratio=2  --command-key-pattern=R > $cmd_get
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="set __key__ __data__" --command-ratio=2  --command-key-pattern=R > $cmd_set

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="hset __key__ __data__ 5" --command-ratio=2  --command-key-pattern=R > $cmd_hset
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="hget __key__ __data__" --command-ratio=2  --command-key-pattern=R > $cmd_hget

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="lpush __key__ __key__ __data__" --command-ratio=2  --command-key-pattern=R > $cmd_lpush
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="rpush __key__ __key__ __data__" --command-ratio=2  --command-key-pattern=R > $cmd_rpush
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="lrange __key__ 0 -1" --command-ratio=2  --command-key-pattern=R > $cmd_lrange

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="sadd __key__ __data__" --command-ratio=2  --command-key-pattern=R > $cmd_sadd
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="smembers __key__" --command-ratio=2  --command-key-pattern=R > $cmd_smembers
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="spop __key__" --command-ratio=2  --command-key-pattern=R > $cmd_spop

memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="zadd __key__ 1 __data__" --command-ratio=2  --command-key-pattern=R > $cmd_zadd
memtier_benchmark --server=$server --port=$port --clients=$clients --requests=$requests --data-size=$dataSize --threads=$threads --select-db=0 --command="zrange __key__ 0 -1" --command-ratio=2  --command-key-pattern=R > $cmd_zrange
