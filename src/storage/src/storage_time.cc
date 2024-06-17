//  Copyright (c) 2024-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <memory>
#include "storage/storage_time.h"
#include <glog/logging.h>

namespace storage {

std::unique_ptr<LogicTime> g_storage_logictime = std::make_unique<LogicTime>();

int64_t LogicTime::Now() {
  if (protection_mode_) {
    return logic_time_;
  }
  int64_t t;
  rocksdb::Env::Default()->GetCurrentTime(&t);
  return t;
}

void LogicTime::UpdateLogicTime(int64_t time) {
  if (protection_mode_) {
    logic_time_ = time;
  }
}

bool LogicTime::ProtectionMode() {
  return protection_mode_;
}

void LogicTime::SetProtectionMode(bool on) {
  protection_mode_ = on;
  LOG(INFO) << "Set protection mode to: " << on;
}

} // namespace storage

