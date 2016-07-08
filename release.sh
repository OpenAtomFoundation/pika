#!/bin/bash

# Used for Release only.

TAG="2.1.0"
make clean && make RPATH=./lib

#TAG="2.0.4"
#make clean && make RPATH=./lib __REL=1

#cp LOOKME output/
#tar -czf $RELEASE_FILE output
tar -czf pika${TAG}_bin.tar.gz output
