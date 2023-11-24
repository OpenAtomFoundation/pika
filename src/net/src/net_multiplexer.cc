// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_multiplexer.h"

#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

#include <glog/logging.h>

#include "pstd/include/xdebug.h"

namespace net {

NetMultiplexer::NetMultiplexer(int queue_limit) : queue_limit_(queue_limit), fired_events_(NET_MAX_CLIENTS) {
  int fds[2];
  if (pipe(fds) != 0) {
    exit(-1);
  }
  notify_receive_fd_ = fds[0];
  notify_send_fd_ = fds[1];

  fcntl(notify_receive_fd_, F_SETFD, fcntl(notify_receive_fd_, F_GETFD) | FD_CLOEXEC);
  fcntl(notify_send_fd_, F_SETFD, fcntl(notify_send_fd_, F_GETFD) | FD_CLOEXEC);
}

NetMultiplexer::~NetMultiplexer() {
  if (multiplexer_ != -1) {
    ::close(multiplexer_);
  }
}

void NetMultiplexer::Initialize() {
  NetAddEvent(notify_receive_fd_, kReadable);
  init_ = true;
}

NetItem NetMultiplexer::NotifyQueuePop() {
  if (!init_) {
    LOG(ERROR) << "please call NetMultiplexer::Initialize()";
    std::abort();
  }

  NetItem it;
  notify_queue_protector_.lock();
  it = notify_queue_.front();
  notify_queue_.pop();
  notify_queue_protector_.unlock();
  return it;
}

bool NetMultiplexer::Register(const NetItem& it, bool force) {
  if (!init_) {
    LOG(ERROR) << "please call NetMultiplexer::Initialize()";
    return false;
  }

  bool success = false;
  notify_queue_protector_.lock();
  if (force || queue_limit_ == kUnlimitedQueue || notify_queue_.size() < static_cast<size_t>(queue_limit_)) {
    notify_queue_.push(it);
    success = true;
  }
  notify_queue_protector_.unlock();
  if (success) {
    ssize_t n = write(notify_send_fd_, "", 1);
    void(n);
  }
  return success;
}

}  // namespace net
