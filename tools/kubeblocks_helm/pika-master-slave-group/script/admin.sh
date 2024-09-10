#! /bin/bash
set -x

# 获取当前Pod的索引
INDEX=${HOSTNAME##*-}
echo "index:${INDEX}"
# PIKA配置文件路径
PIKA_CONF="../data/pika.conf"

# 确保配置文件存在
touch $PIKA_CONF
echo $?
if [ "$INDEX" = "0" ]; then
    # 如果是pika-0，配置为主节点
    ../pika/bin/pika -c ../data/pika.conf
else
    # 如果不是pika-0，配置为从节点
    sed -i "s/#slaveof : master-ip:master-port/slaveof : pika-master-slave-pika-0.pika-master-slave-pika-headless.default.svc.cluster.local:9221/" $PIKA_CONF
    ../pika/bin/pika -c ../data/pika.conf
fi
