//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_RECORD_LOCK_H_
#define SRC_SCOPE_RECORD_LOCK_H_

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "pstd/include/scope_record_lock.h"
#include "src/lock_mgr.h"
#include "storage/storage.h"

namespace storage {

using ScopeRecordLock = pstd::lock::ScopeRecordLock;
using MultiScopeRecordLock = pstd::lock::MultiScopeRecordLock;

}  // namespace storage
#endif  // SRC_SCOPE_RECORD_LOCK_H_
