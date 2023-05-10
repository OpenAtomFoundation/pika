// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STABLE_LOG_H_
#define PIKA_STABLE_LOG_H_

#include <map>
#include <memory>

#include "include/pika_binlog.h"

class StableLog : public std::enable_shared_from_this<StableLog> {
 public:
  StableLog(const std::string table_name, uint32_t partition_id, const std::string& log_path);
  ~StableLog();
  std::shared_ptr<Binlog> Logger() { return stable_logger_; }
  void Leave();
  void SetFirstOffset(const LogOffset& offset) {
    std::lock_guard l(offset_rwlock_);
    first_offset_ = offset;
  }
  LogOffset first_offset() {
    std::shared_lock l(offset_rwlock_);
    return first_offset_;
  }
  // Need to hold binlog lock
  Status TruncateTo(const LogOffset& offset);

  // Purgelogs use
  bool PurgeStableLogs(uint32_t to = 0, bool manual = false);
  void ClearPurge();
  bool GetBinlogFiles(std::map<uint32_t, std::string>* binlogs);
  Status PurgeFileAfter(uint32_t filenum);

 private:
  void Close();
  void RemoveStableLogDir();
  void UpdateFirstOffset(uint32_t filenum);
  /*
   * Purgelogs use
   */
  static void DoPurgeStableLogs(void* arg);
  bool PurgeFiles(uint32_t to, bool manual);
  std::atomic<bool> purging_;

  std::string table_name_;
  uint32_t partition_id_ = 0;
  std::string log_path_;
  std::shared_ptr<Binlog> stable_logger_;

  std::shared_mutex offset_rwlock_;
  LogOffset first_offset_;
};

struct PurgeStableLogArg {
  std::shared_ptr<StableLog> logger;
  uint32_t to = 0;
  bool manual = false;
  bool force = false;  // Ignore the delete window
};

#endif  // PIKA_STABLE_LOG_H_
