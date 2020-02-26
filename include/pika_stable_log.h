// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_STABLE_LOG_H_
#define PIKA_STABLE_LOG_H_

#include <memory>
#include <map>

#include "include/pika_binlog.h"

class StableLog : public std::enable_shared_from_this<StableLog> {
 public:
  StableLog(const std::string table_name,
      uint32_t partition_id, const std::string& log_path);
  ~StableLog();
  std::shared_ptr<Binlog> Logger() {
    return stable_logger_;
  }
  void Leave();
  void SetFirstOffset(const LogOffset& offset) {
    slash::RWLock l(&offset_rwlock_, true);
    first_offset_ = offset;
  }
  LogOffset first_offset() {
    slash::RWLock l(&offset_rwlock_, false);
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
  uint32_t partition_id_;
  std::string log_path_;
  std::shared_ptr<Binlog> stable_logger_;

  pthread_rwlock_t offset_rwlock_;
  LogOffset first_offset_;
};

struct PurgeStableLogArg {
  std::shared_ptr<StableLog> logger;
  uint32_t to;
  bool manual;
  bool force;  // Ignore the delete window
};

#endif  // PIKA_STABLE_LOG_H_
