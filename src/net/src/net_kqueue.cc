// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_kqueue.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

#include <glog/logging.h>

#include "net/include/net_define.h"
#include "pstd/include/xdebug.h"

namespace net {

NetMultiplexer* CreateNetMultiplexer(int limit) { return new NetKqueue(limit); }

NetKqueue::NetKqueue(int queue_limit) : NetMultiplexer(queue_limit) {
  multiplexer_ = ::kqueue();
  LOG(INFO) << "create kqueue";

  if (multiplexer_ < 0) {
    LOG(ERROR) << "kqueue create fail";
    exit(1);
  }

  fcntl(multiplexer_, F_SETFD, fcntl(multiplexer_, F_GETFD) | FD_CLOEXEC);

  events_.resize(NET_MAX_CLIENTS);
}

int NetKqueue::NetAddEvent(int fd, int mask) {
  int cnt = 0;
  struct kevent change[2];

  if ((mask & kReadable) != 0) {
    EV_SET(change + cnt, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
    ++cnt;
  }

  if ((mask & kWritable) != 0) {
    EV_SET(change + cnt, fd, EVFILT_WRITE, EV_ADD, 0, 0, nullptr);
    ++cnt;
  }

  return kevent(multiplexer_, change, cnt, nullptr, 0, nullptr);
}

int NetKqueue::NetModEvent(int fd, int /*old_mask*/, int mask) {
  int ret = NetDelEvent(fd, kReadable | kWritable);
  if (mask == 0) { return ret;
}

  return NetAddEvent(fd, mask);
}

int NetKqueue::NetDelEvent(int fd, int mask) {
  int cnt = 0;
  struct kevent change[2];

  if ((mask & kReadable) != 0) {
    EV_SET(change + cnt, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    ++cnt;
  }

  if ((mask & kWritable) != 0) {
    EV_SET(change + cnt, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    ++cnt;
  }

  if (cnt == 0) { return -1;
}

  return kevent(multiplexer_, change, cnt, nullptr, 0, nullptr);
}

int NetKqueue::NetPoll(int timeout) {
  struct timespec* p_timeout = nullptr;
  struct timespec s_timeout;
  if (timeout >= 0) {
    p_timeout = &s_timeout;
    s_timeout.tv_sec = timeout / 1000;
    s_timeout.tv_nsec = timeout % 1000 * 1000000;
  }

  int num_events = ::kevent(multiplexer_, nullptr, 0, &events_[0], NET_MAX_CLIENTS, p_timeout);
  if (num_events <= 0) { return 0;
}

  for (int i = 0; i < num_events; i++) {
    NetFiredEvent& ev = fired_events_[i];
    ev.fd = events_[i].ident;
    ev.mask = 0;

    if (events_[i].filter == EVFILT_READ) {
      ev.mask |= kReadable;
    }

    if (events_[i].filter == EVFILT_WRITE) {
      ev.mask |= kWritable;
    }

    if ((events_[i].flags & EV_ERROR) != 0) {
      ev.mask |= kErrorEvent;
    }
  }

  return num_events;
}

}  // namespace net
