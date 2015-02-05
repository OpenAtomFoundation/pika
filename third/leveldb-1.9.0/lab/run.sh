#!/bin/sh

rm -rf /tmp/testdb
rm ./a.out
make
./a.out
