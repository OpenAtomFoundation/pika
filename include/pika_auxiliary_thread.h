// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_AUXILIARY_THREAD_H_
#define PIKA_AUXILIARY_THREAD_H_

#include "net/include/net_thread.h"

#include "pstd/include/pstd_mutex.h"

class PikaAuxiliaryThread : public net::Thread {
 public:
  PikaAuxiliaryThread() :
      mu_(),
      cv_(&mu_) {
      set_thread_name("AuxiliaryThread");
  }
  virtual ~PikaAuxiliaryThread();
  pstd::Mutex mu_;
  pstd::CondVar cv_;
 private:
  virtual void* ThreadMain();
};

#endif
