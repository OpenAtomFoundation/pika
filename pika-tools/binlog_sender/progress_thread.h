//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_PROGRESS_THREAD_H_
#define INCLUDE_PROGRESS_THREAD_H_

#include "iostream"
#include "vector"

#include "net/include/net_thread.h"

#include "binlog_consumer.h"

extern slash::Mutex mutex;

class ProgressThread : public net::Thread {
  public:
    ProgressThread(BinlogConsumer* binlog_consumer);
  private:
    virtual void *ThreadMain();
    BinlogConsumer* binlog_consumer_;
};

#endif  //  INCLUDE_PROGRESS_THREAD_H_
