// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_SRC_NET_ITEM_H_
#define NET_SRC_NET_ITEM_H_

#include <string>
#include <sstream>

#include "net/include/net_define.h"

namespace net {

class NetItem {
 public:
  NetItem(); // default constructor, its id is NET_INVALID_ID
  NetItem(const int fd, const std::string& ip_port, const NotifyType& type = kNotiConnect);
  NetItem(const NetID id, const int fd, const std::string& ip_port, const NotifyType& type = kNotiConnect);

  NetID id() const { return id_; }

  void set_fd(int fd) { fd_ = fd; }
  int fd() const { return fd_; }

  std::string ip_port() const { return ip_port_; }

  std::string String() const {
    std::stringstream ss;
    ss << "id:" << id_ << ", fd:" << fd_ << ", ip_port:" << ip_port_ << ", notify_type:" << notify_type_;
    return ss.str();
  }

  NotifyType notify_type() const { return notify_type_; }

 private:
  NetID id_ = -1;
  int fd_ = -1;
  std::string ip_port_ = ""; // peer address
  NotifyType notify_type_ = kNotiConnect;
};

}  // namespace net
#endif  // NET_SRC_NET_ITEM_H_
