// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_PROCESSOR_H_
#define PIKA_CLIENT_PROCESSOR_H_

#include <vector>
#include <string>

#include "pink/include/thread_pool.h"
#include "pink/include/bg_thread.h"

class PikaClientProcessor {
 public:
  PikaClientProcessor(
      size_t worker_num,
      size_t max_queue_size,
      const std::string& name_prefix = "CliProcessor");
  ~PikaClientProcessor();
  int Start();
  void Stop();
  void SchedulePool(pink::TaskFunc func, void* arg);
  void ScheduleBgThreads(
      pink::TaskFunc func, void* arg, const std::string& hash_str);
  size_t ThreadPoolCurQueueSize();

 private:
  pink::ThreadPool* pool_;
  std::vector<pink::BGThread*> bg_threads_;
};
#endif  // PIKA_CLIENT_PROCESSOR_H_
