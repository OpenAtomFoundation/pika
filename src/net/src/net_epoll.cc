// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_epoll.h"

#include <fcntl.h>
#include <linux/version.h>
#include <unistd.h>

#include <glog/logging.h>

#include "net/include/net_define.h"
#include "pstd/include/xdebug.h"

namespace net {

NetMultiplexer* CreateNetMultiplexer(int32_t limit) { return new NetEpoll(limit); }

NetEpoll::NetEpoll(int32_t queue_limit) : NetMultiplexer(queue_limit) {
#if defined(EPOLL_CLOEXEC)
  multiplexer_ = epoll_create1(EPOLL_CLOEXEC);
#else
  multiplexer_ = epoll_create(1024);
#endif

  fcntl(multiplexer_, F_SETFD, fcntl(multiplexer_, F_GETFD) | FD_CLOEXEC);

  if (multiplexer_ < 0) {
    LOG(ERROR) << "epoll create fail";
    exit(1);
  }

  events_.resize(NET_MAX_CLIENTS);
}

int32_t NetEpoll::NetAddEvent(int32_t fd, int32_t mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = 0;

  if (mask & kReadable) {
    ee.events |= EPOLLIN;
  }
  if (mask & kWritable) {
    ee.events |= EPOLLOUT;
    }

  return epoll_ctl(multiplexer_, EPOLL_CTL_ADD, fd, &ee);
}

int32_t NetEpoll::NetModEvent(int32_t fd, int32_t old_mask, int32_t mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = (old_mask | mask);
  ee.events = 0;

  if ((old_mask | mask) & kReadable) {
    ee.events |= EPOLLIN;
  }
  if ((old_mask | mask) & kWritable) {
    ee.events |= EPOLLOUT;
  }
  return epoll_ctl(multiplexer_, EPOLL_CTL_MOD, fd, &ee);
}

int32_t NetEpoll::NetDelEvent(int32_t fd, [[maybe_unused]] int32_t mask) {
  /*
   * Kernel < 2.6.9 need a non null event point to EPOLL_CTL_DEL
   */
  struct epoll_event ee;
  ee.data.fd = fd;
  return epoll_ctl(multiplexer_, EPOLL_CTL_DEL, fd, &ee);
}

int32_t NetEpoll::NetPoll(int32_t timeout) {
  int32_t num_events = epoll_wait(multiplexer_, &events_[0], NET_MAX_CLIENTS, timeout);
  if (num_events <= 0) {
    return 0;
  }

  for (int32_t i = 0; i < num_events; i++) {
    NetFiredEvent& ev = fired_events_[i];
    ev.fd = events_[i].data.fd;
    ev.mask = 0;

    if (events_[i].events & EPOLLIN) {
      ev.mask |= kReadable;
    }

    if (events_[i].events & EPOLLOUT) {
      ev.mask |= kWritable;
    }

    if (events_[i].events & (EPOLLERR | EPOLLHUP)) {
      ev.mask |= kErrorEvent;
    }
  }

  return num_events;
}

}  // namespace net
