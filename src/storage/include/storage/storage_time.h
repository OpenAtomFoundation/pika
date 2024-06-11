//  Copyright (c) 2024-present The storage Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_TIME_H
#define STORAGE_TIME_H

#include <atomic>
#include <memory>
#include <cstdint>
#include <rocksdb/env.h>  // Assuming you're using RocksDB for time functions

namespace storage {

class LogicTime {
 public:
  // Constructor
  LogicTime() : logic_time_(0), protection_mode_(false) {}

  // Get the current logical time or the system time
  int64_t Now();

  // Update the logical time
  void UpdateLogicTime(int64_t time);

  // Get the protection mode status
  bool ProtectionMode();

  // Set the protection mode
  void SetProtectionMode(bool on);

 private:
  std::atomic<int64_t> logic_time_ = 0; // Logical time value
  std::atomic<bool> protection_mode_ = false; // Protection mode flag
};

// Global unique pointer to LogicTime instance
extern std::unique_ptr<LogicTime> g_storage_logictime;

} // namespace storage

#endif // STORAGE_TIME_H

