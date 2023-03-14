#!/bin/sh

if [[ -f ~/pika_port ]]; then
  mv ~/pika_port bin
fi
export LD_LIBRARY_PATH=./bin

# Usage: pika_port [-h] [-t local_ip -p local_port -i master_ip -o master_port -m forward_ip -n forward_port -x forward_thread_num -y forward_passwd]
# -f filenum -s offset -w password -r rsync_dump_path  -l log_path 	-h               -- show this help
# 	-t     -- local host ip(OPTIONAL default: 127.0.0.1)
# 	-p     -- local port(OPTIONAL)
# 	-i     -- master ip(OPTIONAL default: 127.0.0.1)
# 	-o     -- master port(REQUIRED)
# 	-m     -- forward ip(OPTIONAL default: 127.0.0.1)
# 	-n     -- forward port(REQUIRED)
# 	-x     -- forward thread num(OPTIONAL default: 1)
# 	-y     -- forward password(OPTIONAL)
# 	-f     -- binlog filenum(OPTIONAL default: local offset)
# 	-s     -- binlog offset(OPTIONAL default: local offset)
# 	-w     -- password for master(OPTIONAL)
# 	-r     -- rsync dump data path(OPTIONAL default: ./rsync_dump)
# 	-l     -- local log path(OPTIONAL default: ./log)
# 	-d     -- daemonize(OPTIONAL)
#   example: ./pika_port -t 127.0.0.1 -p 12345 -i 127.0.0.1 -o 9221 -m 127.0.0.1 -n 6379 -x 7 -f 0 -s 0 -w abc -l ./log -r ./rsync_dump -d

rm -rf ./rsync_dump/
rm -rf ./sync_log/
./bin/pika_port -t 10.33.80.155 -i 10.33.80.155 -o 13333 -m 10.33.80.155 -n 16379 -x 10 -l ./sync_log/ -r ./rsync_dump
