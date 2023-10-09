#!/bin/bash

BUILD_TIME=$(git log -1 --format=%ai)
BUILD_TIME=${BUILD_TIME: 0: 10}

COMMIT_ID=$(git rev-parse HEAD)
SHORT_COMMIT_ID=${COMMIT_ID: 0: 8}

if [ -z "$SHORT_COMMIT_ID" ]; then
    echo "no git commit id"
    SHORT_COMMIT_ID="pikiwidb"
fi

echo "BUILD_TIME:" $BUILD_TIME
echo "COMMIT_ID:" $SHORT_COMMIT_ID

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TIME=$BUILD_TIME -DGIT_COMMIT_ID=$SHORT_COMMIT_ID -S . -B build
cmake --build build -- -j 32
