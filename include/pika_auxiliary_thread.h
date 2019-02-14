// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_AUXILIARY_THREAD_H_
#define PIKA_AUXILIARY_THREAD_H_

#include "pink/include/pink_thread.h"

class PikaAuxiliaryThread : public pink::Thread {
  public:
    PikaAuxiliaryThread() {}
    virtual ~PikaAuxiliaryThread();

  private:
    virtual void* ThreadMain();
};

#endif
