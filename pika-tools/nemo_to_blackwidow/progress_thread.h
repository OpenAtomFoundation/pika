//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_PROGRESS_THREAD_H_
#define INCLUDE_PROGRESS_THREAD_H_

#include "iostream"
#include "vector"

#include "net/include/net_thread.h"

#include "classify_thread.h"

extern slash::Mutex mutex;

class ProgressThread : public net::Thread {
  public:
    ProgressThread(std::vector<ClassifyThread*>* classify_threads);
  private:
    bool AllClassifyTreadFinish();
    virtual void *ThreadMain();
    std::vector<ClassifyThread*>* classify_threads_;
};

#endif  //  INCLUDE_PROGRESS_THREAD_H_
