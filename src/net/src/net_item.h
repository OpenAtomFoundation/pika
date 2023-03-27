// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_ITEM_H_
#define NET_SRC_NET_ITEM_H_

#include <string>

#include "net/include/net_define.h"

namespace net {

class NetItem {
 public:
  NetItem() {}
  NetItem(const int fd, const std::string &ip_port, const NotifyType& type = kNotiConnect)
      : fd_(fd),
        ip_port_(ip_port),
        notify_type_(type) {
  }

  int fd() const {
    return fd_;
  }
  std::string ip_port() const {
    return ip_port_;
  }

  NotifyType notify_type() const {
    return notify_type_;
  }

 private:
  int fd_;
  std::string ip_port_;
  NotifyType notify_type_;
};

}  // namespace net
#endif  // NET_SRC_NET_ITEM_H_
