//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "progress_thread.h"

ProgressThread::ProgressThread(std::vector<ClassifyThread*>* classify_threads) : classify_threads_(classify_threads) {}

bool ProgressThread::AllClassifyTreadFinish() {
  for (const auto& classify_thread : *classify_threads_) {
    if (!classify_thread->is_finish()) {
      return false;
    }
  }
  return true;
}

void* ProgressThread::ThreadMain() {
  while (true) {
    slash::MutexLock l(&mutex);
    bool is_finish = AllClassifyTreadFinish();
    printf("\rstring keys: %5ld, hashes keys: %5ld, lists keys: %5ld, sets keys: %5ld, zsets keys: %5ld ",
           (*classify_threads_)[0]->key_num(), (*classify_threads_)[1]->key_num(), (*classify_threads_)[2]->key_num(),
           (*classify_threads_)[3]->key_num(), (*classify_threads_)[4]->key_num());
    fflush(stdout);
    if (is_finish) {
      break;
    }
    sleep(1);
  }
  slash::MutexLock l(&mutex);
  printf("\nClassify keys finished\n");
  return NULL;
}
