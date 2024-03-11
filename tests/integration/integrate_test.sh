# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

go mod tidy
#go test -timeout 30m

go get -u github.com/bsm/ginkgo/v2
go get -u github.com/bsm/gomega

echo "GOPATH = $GOPATH"
echo "GO111MODULE = $GO111MODULE"

find / -name ginkgo | grep ginkgo

export PATH=$PATH:$GOPATH/bin

./ginkgo --until-it-fails