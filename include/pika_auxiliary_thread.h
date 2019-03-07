// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_AUXILIARY_THREAD_H_
#define PIKA_AUXILIARY_THREAD_H_

#include "pink/include/pink_thread.h"

#include "slash/include/slash_mutex.h"

class PikaAuxiliaryThread : public pink::Thread {
  public:
    PikaAuxiliaryThread() :
      mu_(),
      cv_(&mu_) {
      }
    virtual ~PikaAuxiliaryThread();
    slash::Mutex mu_;
    slash::CondVar cv_;
  private:
    virtual void* ThreadMain();
    void RunEveryPartitionStateMachine();
};

#endif
