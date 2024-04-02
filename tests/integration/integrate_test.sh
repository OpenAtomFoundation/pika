# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

go mod tidy

echo $PATH
echo $GOBIN

# install ginkgo
go get github.com/onsi/ginkgo/v2/ginkgo
go install github.com/onsi/ginkgo/v2/ginkgo
go get github.com/onsi/gomega/...

ginkgo --dry-run -v |grep -E -v "\[[0-9]+\.[0-9]+ seconds]"

go test -run=TestPikaWithCache -timeout 60m
go test -run=TestPikaWithoutCache -timeout 60m