// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_EPOLL_H_
#define NET_SRC_NET_EPOLL_H_
#include <vector>

#include <sys/epoll.h>

#include "net/src/net_multiplexer.h"

namespace net {

class NetEpoll final : public NetMultiplexer {
 public:
  NetEpoll(int queue_limit = kUnlimitedQueue);
  ~NetEpoll() = default;

  int NetAddEvent(int fd, int mask) override;
  int NetDelEvent(int fd, [[maybe_unused]] int mask) override;
  int NetModEvent(int fd, int old_mask, int mask) override;

  int NetPoll(int timeout) override;

 private:
  std::vector<struct epoll_event> events_;
};

}  // namespace net
#endif  // NET_SRC_NET_EPOLL_H_
