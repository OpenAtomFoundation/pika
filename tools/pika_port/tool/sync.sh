#!/bin/sh

if [[ -f ~/pika_port ]]; then
  mv ~/pika_port bin
fi
export LD_LIBRARY_PATH=./bin

rm -rf ./rsync_dump/
rm -rf ./sync_log/
./bin/pika_port -t 10.1.1.2 -i 10.1.1.1 -o 14443 -m 10.1.1.3 -n 6379 -x 10 -l ./sync_log/ -r ./rsync_dump -b 1024
