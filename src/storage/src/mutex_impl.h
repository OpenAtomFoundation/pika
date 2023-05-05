//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_MUTEX_IMPL_H_
#define SRC_MUTEX_IMPL_H_

#include "src/mutex.h"

#include "pstd/include/mutex_impl.h"

#include <memory>

namespace storage {

using MutexFactoryImpl = pstd::lock::MutexFactoryImpl;

}  //  namespace storage
#endif  // SRC_MUTEX_IMPL_H_
