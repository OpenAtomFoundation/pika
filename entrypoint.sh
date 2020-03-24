#!/bin/sh
#
# CONFIG(path1;path2)
# You can add the path of configuration file or folder to the CONFIG
# environment variable, and use semicolon to separate them. This script
# will catch all SUNKAISENS_ prefix environment variable and match it
# in configuration files. e.g. 'SUNKAISENS_HELLO__ZHANG_XIANG_LONG' may
# be converted to 'hello.zhang_xiang_long'. This is borrowed from the
# emqx's docker-entrypoint.sh.
#
# VAR_CASE(CamelCase,snake_case,spinal-case,ignore-case,raw-case)
# We use 'raw-case' by default, if you set the VAR_CASE environment
# variable to 'CamelCase', that will be converted to 'hello.zhangXiangLong'.
# Of course, we also support case-insensitive variable name matching by
# setting VAR_CASE to 'ignore-case', but it is not recommended because you
# need to ensure that the variable name is unique when the case is ignored.
#
# VAR_SEPARATOR('=' or ':')
# VAR_SEPARATOR environment variable is used to separate the name and
# value of variables, we use '=' by default, you can also use ':'.
#
# WAIT_FOR_SERVICE(IP1[:Port1];IP2[:Port2])
# In addition, you can add services that you want wait for to the WAIT_FOR_SERVICE
# environment variable, such as database service. This script will wait
# for these services until they are available.
#
# ZhangXiangLong <819171011@qq.com>

set -eo pipefail

CONFIG=/pika/conf/pika.conf
VAR_CASE=spinal-case
VAR_SEPARATOR=":"

case $VAR_SEPARATOR in
:)
    ;;
=)
    ;;
*)
    VAR_SEPARATOR="=";;
esac

array=(${CONFIG//;/ })
for path in ${array[@]}; do
    if [[ -f $path ]]; then
        file_list=$file_list";"$path
    elif [[ -d $path ]]; then
        for var in $(ls $path); do
            if [[ -f $path/$var ]]; then
                file_list=$file_list";"$path/$var
            fi
        done
    fi
done
file_array=(${file_list//;/ })

var_replace() {
    for file in ${file_array[@]}; do
        if [[ -n "$(cat $file | grep -E "^(^|^#*|^#*\s*)$var_name")" ]]; then
            echo "$var_name${VAR_SEPARATOR}$(eval echo \$$var_full_name)"
            echo "$(sed -r "s/(^#*\s*)($var_name)\s*${VAR_SEPARATOR}\s*(.*)/\2${VAR_SEPARATOR}$(eval echo \$$var_full_name | sed 's/[[:punct:]]/\\&/g')/g" $file)" > $file
        fi
    done
}

var_replace_i() {
    for file in ${file_array[@]}; do
        if [[ -n "$(cat $file | grep -i -E "^(^|^#*|^#*\s*)$var_name")" ]]; then
            echo "$var_name${VAR_SEPARATOR}$(eval echo \$$var_full_name)"
            echo "$(sed -r "s/(^#*\s*)($var_name)\s*${VAR_SEPARATOR}\s*(.*)/\2${VAR_SEPARATOR}$(eval echo \$$var_full_name | sed 's/[[:punct:]]/\\&/g')/gI" $file)" > $file
        fi
    done
}

for var in $(env); do
    if [[ -n "$(echo $var | grep -E "^PIKA_")" ]]; then
        var_name=$(echo "$var" | sed -r "s/PIKA_([^=]*)=.*/\1/g" | sed -r "s/__/\./g")
        var_full_name=$(echo "$var" | sed -r "s/([^=]*)=.*/\1/g")
        case $VAR_CASE in
        CamelCase)
            var_name=$(echo "$var_name" | tr '[:upper:]' '[:lower:]' | sed -r "s/_([^_])/\u\1/g")
            var_replace;;
        snake_case)
            var_name=$(echo "$var_name" | tr '[:upper:]' '[:lower:]')
            var_replace;;
        spinal-case)
            var_name=$(echo "$var_name" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
            var_replace;;
        ignore-case)
            var_name=$(echo "$var_name" | tr '[:upper:]' '[:lower:]')
            var_replace_i;;
        raw-case | *)
            var_replace;;
        esac
    fi
done

# https://docs.docker.com/compose/startup-order/
# https://github.com/vishnubob/wait-for-it
if [[ -n "$WAIT_FOR_SERVICE" ]]; then
    serv_array=(${WAIT_FOR_SERVICE//;/ })
    for serv in ${serv_array[@]}; do
        serv_addr=(${serv//:/ })
        serv_host=${serv_addr[0]}
        serv_port=${serv_addr[1]}
        case ${#serv_addr[*]} in
        1)
            until host $serv_host; do
                sleep 3
            done
            echo "$serv_host OK.";;
        2)
            until host $serv_host; do
                sleep 3
            done
            echo "$serv_host OK."

            wait-for-it.sh -t 0 $serv;;
        *)
            ;;
        esac
    done
fi

exec "$@"
