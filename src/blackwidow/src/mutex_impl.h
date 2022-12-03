//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_MUTEX_IMPL_H_
#define SRC_MUTEX_IMPL_H_

#include "src/mutex.h"

#include <memory>

namespace blackwidow {

// Default implementation of MutexFactory.
class MutexFactoryImpl : public MutexFactory {
 public:
  std::shared_ptr<Mutex> AllocateMutex() override;
  std::shared_ptr<CondVar> AllocateCondVar() override;
};

}  //  namespace blackwidow
#endif  // SRC_MUTEX_IMPL_H_
