# Copyright (c) 2023-present, Qihoo, Inc. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

start_server {tags {"command"}} {
    test "Command docs supported." {
        set doc [r command docs set]
        # puts $doc
        assert [dict exists $doc set]
    }
}
