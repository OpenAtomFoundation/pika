# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

go mod tidy

echo "GO PATH = $GOPATH"
export PATH=$PATH:$GOPATH/bin

go get -u github.com/bsm/ginkgo/v2
go get -u github.com/bsm/gomega

find / -name ginkgo | grep ginkgo

/home/runner/go/pkg/mod/github.com/bsm/ginkgo --until-it-fails