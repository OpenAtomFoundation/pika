// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_EPOLL_H_
#define NET_SRC_NET_EPOLL_H_
#include <queue>
#include "sys/epoll.h"

#include "net/src/net_item.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

struct NetFiredEvent {
  int fd;
  int mask;
};

class NetEpoll {
 public:
  static const int kUnlimitedQueue = -1;
  NetEpoll(int queue_limit = kUnlimitedQueue);
  ~NetEpoll();
  int NetAddEvent(const int fd, const int mask);
  int NetDelEvent(const int fd);
  int NetModEvent(const int fd, const int old_mask, const int mask);

  int NetPoll(const int timeout);

  NetFiredEvent *firedevent() const { return firedevent_; }

  int notify_receive_fd() {
    return notify_receive_fd_;
  }
  int notify_send_fd() {
    return notify_send_fd_;
  }
  NetItem notify_queue_pop();

  bool Register(const NetItem& it, bool force);
  bool Deregister(const NetItem& it) { return false; }

 private:
  int epfd_;
  struct epoll_event *events_;
  int timeout_;
  NetFiredEvent *firedevent_;

  /*
   * The PbItem queue is the fd queue, receive from dispatch thread
   */
  int queue_limit_;
  pstd::Mutex notify_queue_protector_;
  std::queue<NetItem> notify_queue_;

  /*
   * These two fd receive the notify from dispatch thread
   */
  int notify_receive_fd_;
  int notify_send_fd_;
};

}  // namespace net
#endif  // NET_SRC_NET_EPOLL_H_
