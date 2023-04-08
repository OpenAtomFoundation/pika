#!/bin/bash

MIN_VERSION="3.10"

BUILD_DIR=output

source ./utils/Get_OS_Version.sh

function version_compare() {
    if [[ "${MIN_VERSION}" == "$1" ]]; then
        return 0
    fi

    if [[ "$(printf '%s\n' "${MIN_VERSION}" "$1" | sort -rV | head -n1)" == "${MIN_VERSION}" ]]; then
        #local version less min version
        echo -e "local cmake version \033[32m $1 \033[0m less min version \033[32m ${MIN_VERSION} \033[0m"
        exit 1
    fi
}

if ! type autoconf >/dev/null 2>&1; then
    # not find autoconf,do install
    echo -e "not find \033[32m autoconf \033[0m on localhost, now do install"
    if [ $PM == "unknow" ]; then
        echo -e "\033[31m unknow package manager, please install autoconf \033[0m"
    fi
    if [ ${PM} == "apt" ]; then
      sudo ${PM} -y install autoconf
    elif [ ${PM} == "brew" ]; then
      ${PM} install -d autoconf
    else
      sudo ${PM} install -y autoconf
    fi
    if [ $? -ne 0 ]; then
        echo -e "\033[31m install autoconf  fail, install autoconf before compiling \033[0m"
        exit 1;
    fi
fi

if ! type cmake >/dev/null 2>&1; then
    if ! type cmake >/dev/null 2>&1; then
        echo "not find cmake, please install cmake and min version \033[32m ${MIN_VERSION} \033[0m"
        exit 1
    else
        CMAKE=cmake3
    fi
else
    CMAKE=cmake
fi

LOCAL_VERSION=`${CMAKE} --version |grep version |grep -o '[0-9.]\+'`

version_compare ${LOCAL_VERSION}

if [ ! -d ${BUILD_DIR} ]; then
    mkdir ${BUILD_DIR}
fi

cd ${BUILD_DIR}

${CMAKE} .. .

if [ $? -ne 0 ]; then
    echo -e "\033[31m cmake execution error \033[0m"
    exit 1
fi

CPU_CORE=`cat /proc/cpuinfo| grep "processor"| wc -l`
if [ ${CPU_CORE} -eq 0 ]; then
  CPU_CORE=1
fi

echo "cpu core ${CPU_CORE}"

make -j ${CPU_CORE}

if [ $? -eq 0 ]; then
    echo -e "pika compile complete, output file \033[32m ${BUILD_DIR}/pika \033[0m"
fi
