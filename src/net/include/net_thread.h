// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_NET_THREAD_H_
#define NET_INCLUDE_NET_THREAD_H_

#include <pthread.h>
#include <atomic>
#include <string>

#include "pstd/include/pstd_mutex.h"
#include "pstd/include/noncopyable.h"

namespace net {

class Thread : public pstd::noncopyable {
 public:
  Thread();
  virtual ~Thread();

  virtual int StartThread();
  virtual int StopThread();
  int JoinThread();

  bool should_stop() { return should_stop_.load(); }

  void set_should_stop() { should_stop_.store(true); }

  bool is_running() { return running_.load(); }

  pthread_t thread_id() const { return thread_id_; }

  std::string thread_name() const { return thread_name_; }

  void set_thread_name(const std::string& name) { thread_name_ = name; }

 protected:
  std::atomic_bool should_stop_;
  void set_is_running(bool is_running) {
    std::lock_guard l(running_mu_);
    running_ = is_running;
  }

 private:
  static void* RunThread(void* arg);
  virtual void* ThreadMain() = 0;

  pstd::Mutex running_mu_;
  std::atomic_bool running_ = false;
  pthread_t thread_id_{};
  std::string thread_name_;
};

}  // namespace net
#endif  // NET_INCLUDE_NET_THREAD_H_
