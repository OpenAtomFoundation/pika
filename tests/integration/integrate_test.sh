# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

go mod tidy
#go test -timeout 30m

export GOPATH=~
export PATH=$PATH:$GOPATH/bin

go install github.com/bsm/ginkgo/v2@v2.12.0
#go install github.com/bsm/gomega/..

echo "GO111MODULE = $GO111MODULE"
echo "GOPATH = $GOPATH"
echo "whoami = $(whoami)"

find / -name ginkgo | grep ginkgo

echo "before=$(which ginkgo)"

ginkgo --until-it-fails

