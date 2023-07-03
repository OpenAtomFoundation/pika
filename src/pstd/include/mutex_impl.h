//  Copyright (c) 2017-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __SRC_MUTEX_IMPL_H__
#define __SRC_MUTEX_IMPL_H__

#include <memory>

#include "pstd/include/mutex.h"

namespace pstd {
namespace lock {
// Default implementation of MutexFactory.
class MutexFactoryImpl : public MutexFactory {
 public:
  std::shared_ptr<Mutex> AllocateMutex() override;
  std::shared_ptr<CondVar> AllocateCondVar() override;
};
}  //  namespace lock
}  //  namespace pstd
#endif  // SRC_MUTEX_IMPL_H__
