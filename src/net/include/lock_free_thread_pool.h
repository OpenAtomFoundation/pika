// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_LOCK_FREE_THREAD_POOL_H_
#define NET_INCLUDE_LOCK_FREE_THREAD_POOL_H_

#include <pthread.h>
#include <atomic>
#include <queue>
#include <string>

#include "net/include/thread_pool.h"
#include "net/include/net_define.h"
#include "concurrentqueue/moodycamel/concurrentqueue.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

class LockFreeThreadPool : public pstd::noncopyable {
 public:
  class Worker {
   public:
    explicit Worker(LockFreeThreadPool* tp) : start_(false), thread_pool_(tp){};
    static void* WorkerMain(void* arg);

    int start();
    int stop();

   private:
    pthread_t thread_id_;
    std::atomic<bool> start_;
    LockFreeThreadPool* const thread_pool_;
    std::string worker_name_;
  };

  explicit LockFreeThreadPool(size_t worker_num, size_t max_queue_size, const std::string& thread_pool_name = "LockFreeThreadPool");
  virtual ~LockFreeThreadPool();

  int start_thread_pool();
  int stop_thread_pool();
  bool should_stop();
  void set_should_stop();

  void Schedule(TaskFunc func, void* arg);
  size_t max_queue_size();
  size_t worker_size();
  void cur_queue_size(size_t* qsize);
  std::string thread_pool_name();

 private:
  void runInThread();

  size_t worker_num_;
  size_t max_queue_size_;
  std::string thread_pool_name_;
  moodycamel::ConcurrentQueue<Task> queue_;
  std::vector<Worker*> workers_;
  std::atomic<bool> running_;
  std::atomic<bool> should_stop_;

  pstd::Mutex mu_;
  pstd::CondVar rsignal_;
  pstd::CondVar wsignal_;

};

}  // namespace net

#endif  // NET_INCLUDE_LOCK_FREE_THREAD_POOL_H_
