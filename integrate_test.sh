# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#!/bin/bash

set -e
set -x

echo 'start integrate-test'

# set root workspace
ROOT_DIR=$(pwd)
echo "integrate-test root work-space -> ${ROOT_DIR}"

# show all github-env
echo "github current commit id  -> $2"
echo "github pull request branch -> ${GITHUB_REF}"
echo "github pull request slug -> ${GITHUB_REPOSITORY}"
echo "github pull request repo slug -> ${GITHUB_REPOSITORY}"
echo "github pull request actor -> ${GITHUB_ACTOR}"
echo "github pull request repo param -> $1"
echo "github pull request base branch -> $3"
echo "github pull request head branch -> ${GITHUB_HEAD_REF}"

git clone -b master https://github.com/luky116/pika-integration.git pika-integration && cd pika-integration

chmod +x ./start_integrate_test.sh
# start integrate test
./start_integrate_test.sh
