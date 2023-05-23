// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_thread.h"
#include "net/include/net_define.h"
#include "net/src/net_thread_name.h"
#include "pstd/include/xdebug.h"

namespace net {

Thread::Thread() : should_stop_(false) {}

Thread::~Thread() = default;

void* Thread::RunThread(void* arg) {
  auto thread = reinterpret_cast<Thread*>(arg);
  if (!(thread->thread_name().empty())) {
    SetThreadName(pthread_self(), thread->thread_name());
  }
  thread->ThreadMain();
  return nullptr;
}

int Thread::StartThread() {
  std::lock_guard l(running_mu_);
  should_stop_ = false;
  if (!running_) {
    running_ = true;
    return pthread_create(&thread_id_, nullptr, RunThread, this);
  }
  return 0;
}

int Thread::StopThread() {
  std::lock_guard l(running_mu_);
  should_stop_ = true;
  if (running_) {
    running_ = false;
    return pthread_join(thread_id_, nullptr);
  }
  return 0;
}

int Thread::JoinThread() { return pthread_join(thread_id_, nullptr); }

}  // namespace net
