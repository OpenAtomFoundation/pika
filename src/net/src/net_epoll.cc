// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_epoll.h"

#include <fcntl.h>
#include <linux/version.h>
#include <unistd.h>

#include "net/include/net_define.h"
#include "pstd/include/xdebug.h"

namespace net {

NetMultiplexer* CreateNetMultiplexer(int limit) { return new NetEpoll(limit); }

NetEpoll::NetEpoll(int queue_limit) : NetMultiplexer(queue_limit) {
#if defined(EPOLL_CLOEXEC)
  multiplexer_ = epoll_create1(EPOLL_CLOEXEC);
#else
  multiplexer_ = epoll_create(1024);
#endif

  fcntl(multiplexer_, F_SETFD, fcntl(multiplexer_, F_GETFD) | FD_CLOEXEC);

  if (multiplexer_ < 0) {
    log_err("epoll create fail");
    exit(1);
  }

  events_.resize(NET_MAX_CLIENTS);
}

int NetEpoll::NetAddEvent(int fd, int mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = 0;

  if (mask & kReadable) ee.events |= EPOLLIN;
  if (mask & kWritable) ee.events |= EPOLLOUT;

  return epoll_ctl(multiplexer_, EPOLL_CTL_ADD, fd, &ee);
}

int NetEpoll::NetModEvent(int fd, int old_mask, int mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = (old_mask | mask);
  ee.events = 0;

  if ((old_mask | mask) & kReadable) ee.events |= EPOLLIN;
  if ((old_mask | mask) & kWritable) ee.events |= EPOLLOUT;
  return epoll_ctl(multiplexer_, EPOLL_CTL_MOD, fd, &ee);
}

int NetEpoll::NetDelEvent(int fd, int) {
  /*
   * Kernel < 2.6.9 need a non null event point to EPOLL_CTL_DEL
   */
  struct epoll_event ee;
  ee.data.fd = fd;
  return epoll_ctl(multiplexer_, EPOLL_CTL_DEL, fd, &ee);
}

int NetEpoll::NetPoll(int timeout) {
  int num_events = epoll_wait(multiplexer_, &events_[0], NET_MAX_CLIENTS, timeout);
  if (num_events <= 0) return 0;

  for (int i = 0; i < num_events; i++) {
    NetFiredEvent& ev = fired_events_[i];
    ev.item.set_fd(events_[i].data.fd);
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
