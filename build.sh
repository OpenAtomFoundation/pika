#!/bin/bash

set -x

#color code
C_RED="\033[31m"
C_GREEN="\033[32m"

C_END="\033[0m"

CMAKE_MIN_VERSION="3.18"
TAR_MIN_VERSION="1.26"

BUILD_DIR=output

source ./utils/Get_OS_Version.sh

function version_compare() {
    if [[ "$1" == "$2" ]]; then
        return 0
    fi

    if [[ "$(printf '%s\n' "$1" "$2" | sort -rV | head -n1)" == "$1" ]]; then
        #local version less min version
        echo -e "local ${C_GREEN} $3 ${C_END} version ${C_GREEN} $2 ${C_END} less min version ${C_GREEN} $1 ${C_END}"
        exit 1
    fi
}

function check_program() {
    if ! type $1 >/dev/null 2>&1; then
        # not find
        echo -e "not find ${C_GREEN} $1 ${C_END} on localhost"
        return 1
    fi
    return 0
}

function install_package() {
    if [ $PM == "unknow" ]; then
        echo -e "${C_RED} unknow package manager, please install $1 ${C_END}"
        exit 1
    fi
    if [ ${PM} == "apt" ]; then
      sudo ${PM} -y install $1
    elif [ ${PM} == "brew" ]; then
      ${PM} install -d $1
    else
      sudo ${PM} install -y $1
    fi
    if [ $? -ne 0 ]; then
        echo -e "${C_RED} install $1  fail, install autoconf before compiling ${C_END}"
        exit 1;
    fi
}

if ! check_program autoconf; then
    # not find autoconf,do install
    echo -e "not find ${C_GREEN} autoconf ${C_END} on localhost, now do install"
    install_package autoconf
fi

if ! check_program tar; then
    echo -e "not find ${C_GREEN} tar ${C_END} on localhost, please install and min version ${C_GREEN} ${TAR_MIN_VERSION} ${C_END}"
    exit 1;
fi

if ! check_program cmake; then
    if ! check_program cmake3; then
        echo -e "not find ${C_GREEN} cmake ${C_END}, please install cmake and min version ${C_GREEN} ${CMAKE_MIN_VERSION} ${C_END}"
        exit 1
    else
        CMAKE=cmake3
    fi
else
    CMAKE=cmake
fi

# get local cmake version
LOCAL_CMAKE_VERSION=`${CMAKE} --version |grep version |grep -o '[0-9.]\+'`
#compare cmake version
version_compare ${CMAKE_MIN_VERSION} ${LOCAL_CMAKE_VERSION} 'cmake'

# get local tar version
LOCAL_TAR_VERSION=`tar --version |head -n 1 |grep -o '[0-9.]\+'`
#compare tar version
version_compare ${TAR_MIN_VERSION} ${LOCAL_TAR_VERSION} 'tar'

if [ ! -d ${BUILD_DIR} ]; then
    mkdir ${BUILD_DIR}
fi

cd ${BUILD_DIR}

use_pika_tools=""
if [[ $1 = "tools" ]]; then
  use_pika_tools="-DUSE_PIKA_TOOLS=ON"
fi

with_command_docs=""
if [ "${WITH_COMMAND_DOCS}" = "ON" ]; then
  with_command_docs="-DWITH_COMMAND_DOCS=ON"
fi

${CMAKE} ${use_pika_tools} ${with_command_docs} .. .

if [ $? -ne 0 ]; then
    echo -e "${C_RED} cmake execution error ${C_END}"
    exit 1
fi

if [ ! -f "/proc/cpuinfo" ];then
  CPU_CORE=$(sysctl -n hw.ncpu)
else
  CPU_CORE=$(cat /proc/cpuinfo| grep "processor"| wc -l)
fi
if [ ${CPU_CORE} -eq 0 ]; then
  CPU_CORE=1
fi

echo "cpu core ${CPU_CORE}"

make -j ${CPU_CORE}

if [ $? -eq 0 ]; then
    echo -e "pika compile complete, output file ${C_GREEN} ${BUILD_DIR}/pika ${C_END}"
else
    echo -e "${C_RED} pika compile fail ${C_END}"
    exit 1
fi
