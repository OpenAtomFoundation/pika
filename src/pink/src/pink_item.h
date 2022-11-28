// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PINK_SRC_PINK_ITEM_H_
#define PINK_SRC_PINK_ITEM_H_

#include <string>

#include "pink/include/pink_define.h"

namespace pink {

class PinkItem {
 public:
  PinkItem() {}
  PinkItem(const int fd, const std::string &ip_port, const NotifyType& type = kNotiConnect)
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

}  // namespace pink
#endif  // PINK_SRC_PINK_ITEM_H_
