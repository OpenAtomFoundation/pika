#!/bin/bash
#author justforfun
rdb_filename=dump.rdb
if [ $# -gt 0 ]
then
rdb_filename=$1
fi
rdb -c protocol -f protocol_file ${rdb_filename}
