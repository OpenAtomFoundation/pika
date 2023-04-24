// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/src/net_item.h"

#include <atomic>

namespace net {

static std::atomic_int64_t g_id = {0};

NetItem::NetItem() {
  id_ = NET_INVALID_ID;
}

NetItem::NetItem(const int fd, const std::string& ip_port, const NotifyType& type)
    : fd_(fd), ip_port_(ip_port), notify_type_(type) {
  id_ = NetID(g_id++);
}

NetItem::NetItem(const NetID id, const int fd, const std::string& ip_port, const NotifyType& type)
    : id_(id), fd_(fd), ip_port_(ip_port), notify_type_(type) {}

}
