#!/bin/bash

set -e

echo "[ BUILD RELEASE ]"
BIN_DIR=$(pwd)/bin/
rm -rf "$BIN_DIR"
mkdir -p "$BIN_DIR"

# build the current platform
echo "try build for current platform"
go build -v -trimpath  -gcflags '-N -l' -o "$BIN_DIR/codis2pika" "./cmd/codis2pika"
echo "build success"

for g in "linux" "darwin"; do
  for a in "amd64" "arm64"; do
    echo "try build GOOS=$g GOARCH=$a"
    export GOOS=$g
    export GOARCH=$a
    go build -v -trimpath  -gcflags '-N -l' -o "$BIN_DIR/codis2pika-$g-$a" "./cmd/codis2pika"
    unset GOOS
    unset GOARCH
    echo "build success"
  done
done

cp codis2pika.toml "$BIN_DIR"

if [ "$1" == "dist" ]; then
  echo "[ DIST ]"
  cd bin
  cp -r ../filters ./
  tar -czvf ./codis2pika.tar.gz ./codis2pika.toml  ./codis2pika-* ./filters
  rm -rf ./filters
  cd ..
fi
