// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pink/include/pink_thread.h"
#include "pink/src/pink_thread_name.h"
#include "pstd/include/xdebug.h"
#include "pink/include/pink_define.h"

namespace pink {

Thread::Thread()
  : should_stop_(false),
    running_(false),
    thread_id_(0) {
}

Thread::~Thread() {
}

void* Thread::RunThread(void *arg) {
  Thread* thread = reinterpret_cast<Thread*>(arg);
  if (!(thread->thread_name().empty())) {
    SetThreadName(pthread_self(), thread->thread_name());
  }
  thread->ThreadMain();
  return nullptr;
}

int Thread::StartThread() {
  pstd::MutexLock l(&running_mu_);
  should_stop_ = false;
  if (!running_) {
    running_ = true;
    return pthread_create(&thread_id_, nullptr, RunThread, (void *)this);
  }
  return 0;
}

int Thread::StopThread() {
  pstd::MutexLock l(&running_mu_);
  should_stop_ = true;
  if (running_) {
    running_ = false;
    return pthread_join(thread_id_, nullptr);
  }
  return 0;
}

int Thread::JoinThread() {
  return pthread_join(thread_id_, nullptr);
}

}  // namespace pink
