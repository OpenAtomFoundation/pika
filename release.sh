#!/bin/bash

# Used for Release only.

TAG="2.1.2"
make clean && make RPATH=./lib __REL=1

tar -czf pika${TAG}_bin.tar.gz output
