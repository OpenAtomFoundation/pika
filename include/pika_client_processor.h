// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_PROCESSOR_H_
#define PIKA_CLIENT_PROCESSOR_H_

#include <memory>
#include <string>
#include <vector>

#include "net/include/bg_thread.h"
#include "net/include/thread_pool.h"

class PikaClientProcessor {
 public:
  PikaClientProcessor(size_t worker_num, size_t max_queue_size,
                      const std::string& name_prefix = "CliProcessor");
  ~PikaClientProcessor();
  int Start();
  void Stop();
  void SchedulePool(net::TaskFunc func, void* arg);
  void ScheduleBgThreads(net::TaskFunc func, void* arg,
                         const std::string& hash_str);
  size_t ThreadPoolCurQueueSize();

 private:
  std::unique_ptr<net::ThreadPool> pool_;
  std::vector<std::unique_ptr<net::BGThread>> bg_threads_;
};
#endif  // PIKA_CLIENT_PROCESSOR_H_
