//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "progress_thread.h"

ProgressThread::ProgressThread(BinlogConsumer* binlog_consumer) : binlog_consumer_(binlog_consumer) {}

void* ProgressThread::ThreadMain() {
  while (!should_stop_) {
    printf("\rSending binlog, current file num: %d, offset: %9ld", binlog_consumer_->current_filenum(),
           binlog_consumer_->current_offset());
    fflush(stdout);
  }
  printf("\n");
  return nullptr;
}
