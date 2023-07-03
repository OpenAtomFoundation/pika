//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "progress_thread.h"

ProgressThread::ProgressThread(ScanThread* scan_thread)
    : scan_thread_(scan_thread) {}

void* ProgressThread::ThreadMain() {
  while (true) {
    bool is_finish = scan_thread_->is_finish();
    printf("\rstrings items: %5d", scan_thread_->scan_number());
    fflush(stdout);
    if (is_finish) {
      break;
    }
  }
  printf("\nScan strings finished\n");
  return nullptr;
}
