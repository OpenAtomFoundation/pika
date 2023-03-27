//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_PROGRESS_THREAD_H_
#define INCLUDE_PROGRESS_THREAD_H_

#include "iostream"
#include "vector"

#include "net/include/net_thread.h"

#include "scan_thread.h"

class ProgressThread : public net::Thread {
 public:
  ProgressThread(ScanThread* scan_thread);
 private:
  bool AllClassifyTreadFinish();
  virtual void *ThreadMain();
  ScanThread* scan_thread_;
};

#endif  //  INCLUDE_PROGRESS_THREAD_H_
