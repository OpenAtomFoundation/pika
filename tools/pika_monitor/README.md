
## README

#### Install

Modify Makefile:
set HIREDIS_INCLUDE option to the location of hiredis head file
set HIREDIS_LIB option to the location of hiredis static lib

#### USAGE:

DESCRIPTION:
 - Redis monitor copy tool: monitor redis server indicated by src_host, src_port, src_auth and send to des server
Parameters: 
   -s: source server 
   -d: destination server 
   -v: show more information 
   -h: help 
Example: 
 - ./redis-copy -s abc@xxx.xxx.xxx.xxx:6379 -d cba@xxx.xxx.xxx.xxx:6379 -v
