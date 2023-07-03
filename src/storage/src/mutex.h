//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_MUTEX_H_
#define SRC_MUTEX_H_

#include <memory>

#include "pstd/include/mutex.h"
#include "rocksdb/status.h"

namespace storage {

using Status = rocksdb::Status;

using Mutex = pstd::lock::Mutex;
using CondVar = pstd::lock::CondVar;
using MutexFactory = pstd::lock::MutexFactory;

}  // namespace storage
#endif  // SRC_MUTEX_H_
