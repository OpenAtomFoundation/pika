#!/bin/bash

function install_deps() {
  echo "install deps before ..."
  if [[ $OS == *"macos"* ]]; then
    brew update
    brew install --overwrite python@3.12 autoconf protobuf llvm wget git
    brew install gcc@10 automake cmake make binutils
  elif [[ $OS == *"ubuntu"* ]]; then
    sudo apt-get install -y autoconf libprotobuf-dev protobuf-compiler
    sudo apt-get install -y clang-tidy-12
  else
    echo "not support $OS"
  fi
  echo "install deps after success ..."
}

function configure_cmake() {
  echo "configure cmake before ..."
  if [[ $OS == *"macos"* ]]; then
    export CC=/usr/local/opt/gcc@10/bin/gcc-10
    cmake -B build -DCMAKE_C_COMPILER=/usr/local/opt/gcc@10/bin/gcc-10 -DUSE_PIKA_TOOLS=ON -DCMAKE_BUILD_TYPE=$BUILD_TYPE
  elif [[ $OS == *"ubuntu"* ]]; then
    cmake -B build -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DUSE_PIKA_TOOLS=ON -DCMAKE_CXX_FLAGS="-s" -DCMAKE_EXE_LINKER_FLAGS="-s"
  elif [[ $OS == *"centos"* ]]; then
    source /opt/rh/devtoolset-10/enable
    cmake -B build -DCMAKE_BUILD_TYPE=$BUILD_TYPE -DUSE_PIKA_TOOLS=ON -DCMAKE_CXX_FLAGS_DEBUG=-fsanitize=address
  fi
  echo "configure cmake after ..."
}

function build() {
  echo "build before ..."
  cmake --build build --config $BUILD_TYPE
  echo "build after success ..."
}

function install() {
  install_deps
  configure_cmake
  build
}

function checksum() {
  cd build/ && chmod +x $REPO_NAME

  mkdir bin && cp $REPO_NAME bin
  mkdir conf && cp ../conf/pika.conf conf

  tar -zcvf $PACKAGE_NAME bin/$REPO_NAME conf/pika.conf

  echo $(shasum -a 256 $PACKAGE_NAME | cut -f1 -d' ') >${PACKAGE_NAME}.sha256sum
}

case $1 in
"install")
  OS=$2
  BUILD_TYPE=$3
  install
  ;;
"checksum")
  REPO_NAME=$2
  PACKAGE_NAME=$3
  checksum
  ;;
*)
  echo "Invalid option"
  echo "option $1"
  ;;
esac
