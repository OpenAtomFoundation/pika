#!/bin/bash

threads=`ps -eLf | grep pika | grep -v 'tmux' | grep -v 'grep' | awk '{print $4}'`
processor_nums=$(cat /proc/cpuinfo | grep processor | wc -l)

i=0
j=0

for thread in $threads
do
  ((j=i%($processor_nums)))
  taskset -pc $j $thread
  let ++i
done
