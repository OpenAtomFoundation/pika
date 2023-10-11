
## README

#### Install
请先确保本机安装了`hiredis`

##### CentOS

```
sudo yum install hiredis-devel
```

修改 Makefile:

设置`HIREDIS_INCLUDE` 为`hiredis`头文件所在位置

设置`HIREDIS_LIB`为`hiredis`静态库所在位置

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