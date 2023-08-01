// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_MULTIPLEXER_H_
#define NET_SRC_NET_MULTIPLEXER_H_
#include <queue>
#include <vector>

#include "net/src/net_item.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

struct NetFiredEvent {
  int32_t fd = -1;
  int32_t mask = 0;  // EventStatus
};

class NetMultiplexer {
 public:
  explicit NetMultiplexer(int32_t queue_limit);
  virtual ~NetMultiplexer();

  virtual int32_t NetAddEvent(int32_t fd, int32_t mask) = 0;
  virtual int32_t NetDelEvent(int32_t fd, int32_t mask) = 0;
  virtual int32_t NetModEvent(int32_t fd, int32_t old_mask, int32_t mask) = 0;
  virtual int32_t NetPoll(int32_t timeout) = 0;

  void Initialize();

  NetFiredEvent* FiredEvents() { return &fired_events_[0]; }

  int32_t NotifyReceiveFd() const { return notify_receive_fd_; }
  int32_t NotifySendFd() const { return notify_send_fd_; }
  NetItem NotifyQueuePop();

  bool Register(const NetItem& it, bool force);

  static const int32_t kUnlimitedQueue = -1;

  int32_t GetMultiplexer(){
    return multiplexer_;
  }
 protected:
  int32_t multiplexer_ = -1;
  /*
   * The PbItem queue is the fd queue, receive from dispatch thread
   */
  int32_t queue_limit_ = kUnlimitedQueue;
  pstd::Mutex notify_queue_protector_;
  std::queue<NetItem> notify_queue_;
  std::vector<NetFiredEvent> fired_events_;

  /*
   * These two fd receive the notify from dispatch thread
   */
  int32_t notify_receive_fd_ = -1;
  int32_t notify_send_fd_ = -1;

  bool init_ = false;
};

NetMultiplexer* CreateNetMultiplexer(int32_t queue_limit = NetMultiplexer::kUnlimitedQueue);

}  // namespace net
#endif  // NET_SRC_NET_EPOLL_H_
