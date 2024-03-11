# Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

go mod tidy

go get github.com/bsm/ginkgo/v2
go get github.com/bsm/gomega

ginkgo --randomize-all --seed=23456 --fail-on-pending --cover --progress --trace --race --skipPackage=foo --until-it-fails