// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pink/src/pink_epoll.h"

#include <linux/version.h>
#include <fcntl.h>

#include "pink/include/pink_define.h"
#include "slash/include/xdebug.h"

namespace pink {

static const int kPinkMaxClients = 10240;

PinkEpoll::PinkEpoll(int queue_limit) : timeout_(1000), queue_limit_(queue_limit) {
#if defined(EPOLL_CLOEXEC)
    epfd_ = epoll_create1(EPOLL_CLOEXEC);
#else
    epfd_ = epoll_create(1024);
#endif

  fcntl(epfd_, F_SETFD, fcntl(epfd_, F_GETFD) | FD_CLOEXEC);

  if (epfd_ < 0) {
    log_err("epoll create fail");
    exit(1);
  }
  events_ = (struct epoll_event *)malloc(
      sizeof(struct epoll_event) * kPinkMaxClients);

  firedevent_ = reinterpret_cast<PinkFiredEvent*>(malloc(
      sizeof(PinkFiredEvent) * kPinkMaxClients));

  int fds[2];
  if (pipe(fds)) {
    exit(-1);
  }
  notify_receive_fd_ = fds[0];
  notify_send_fd_ = fds[1];

  fcntl(notify_receive_fd_, F_SETFD, fcntl(notify_receive_fd_, F_GETFD) | FD_CLOEXEC);
  fcntl(notify_send_fd_, F_SETFD, fcntl(notify_send_fd_, F_GETFD) | FD_CLOEXEC);

  PinkAddEvent(notify_receive_fd_, EPOLLIN | EPOLLERR | EPOLLHUP);
}

PinkEpoll::~PinkEpoll() {
  free(firedevent_);
  free(events_);
  close(epfd_);
}

int PinkEpoll::PinkAddEvent(const int fd, const int mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = mask;
  return epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ee);
}

int PinkEpoll::PinkModEvent(const int fd, const int old_mask, const int mask) {
  struct epoll_event ee;
  ee.data.fd = fd;
  ee.events = (old_mask | mask);
  return epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ee);
}

int PinkEpoll::PinkDelEvent(const int fd) {
  /*
   * Kernel < 2.6.9 need a non null event point to EPOLL_CTL_DEL
   */
  struct epoll_event ee;
  ee.data.fd = fd;
  return epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, &ee);
}

bool PinkEpoll::Register(const PinkItem& it, bool force) {
  bool success = false;
  notify_queue_protector_.Lock();
  if (force ||
      queue_limit_ == kUnlimitedQueue ||
      notify_queue_.size() < static_cast<size_t>(queue_limit_)) {
    notify_queue_.push(it);
    success = true;
  }
  notify_queue_protector_.Unlock();
  if (success) {
    write(notify_send_fd_, "", 1);
  }
  return success;
}

PinkItem PinkEpoll::notify_queue_pop() {
  PinkItem it;
  notify_queue_protector_.Lock();
  it = notify_queue_.front();
  notify_queue_.pop();
  notify_queue_protector_.Unlock();
  return it;
}

int PinkEpoll::PinkPoll(const int timeout) {
  int retval, numevents = 0;
  retval = epoll_wait(epfd_, events_, PINK_MAX_CLIENTS, timeout);
  if (retval > 0) {
    numevents = retval;
    for (int i = 0; i < numevents; i++) {
      int mask = 0;
      firedevent_[i].fd = (events_ + i)->data.fd;

      if ((events_ + i)->events & EPOLLIN) {
        mask |= EPOLLIN;
      }
      if ((events_ + i)->events & EPOLLOUT) {
        mask |= EPOLLOUT;
      }
      if ((events_ + i)->events & EPOLLERR) {
        mask |= EPOLLERR;
      }
      if ((events_ + i)->events & EPOLLHUP) {
        mask |= EPOLLHUP;
      }
      firedevent_[i].mask = mask;
    }
  }
  return numevents;
}

}  // namespace pink
