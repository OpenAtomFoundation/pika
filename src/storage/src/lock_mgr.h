//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LOCK_MGR_H_
#define SRC_LOCK_MGR_H_

#include <memory>
#include <string>

#include "pstd/include/lock_mgr.h"

#include "src/mutex.h"

namespace storage {

using LockMgr = pstd::lock::LockMgr;

}  //  namespace storage
#endif  // SRC_LOCK_MGR_H_
