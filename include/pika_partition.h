// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PARTITION_H_
#define PIKA_PARTITION_H_

#include <string>
#include <memory>

#include "iostream"

#include "blackwidow/blackwidow.h"

#include "include/pika_conf.h"
#include "include/pika_binlog.h"

class Cmd;
class Binlog;

class Partition : public std::enable_shared_from_this<Partition> {
 public:
  Partition(const std::string& table_name,
            uint32_t partition_id,
            const std::string& table_db_path,
            const std::string& table_log_path);
  virtual ~Partition();

  uint32_t partition_id() const;
  std::string partition_name() const;
  std::shared_ptr<Binlog> logger() const;
  std::shared_ptr<blackwidow::BlackWidow> db() const;

  void DoCommand(Cmd* const cmd);

  void BinlogLock();
  void BinlogUnLock();
  void DbRWLockWriter();
  void DbRWLockReader();
  void DbRWUnLock();
  void RecordLock(const std::string& key);
  void RecordUnLock(const std::string& key);

  void SetBinlogIoError(bool error);
  bool IsBinlogIoError();

  void RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option) const;


 private:
  std::string table_name_;
  uint32_t partition_id_;

  std::string db_path_;
  std::string log_path_;
  std::string partition_name_;

  std::shared_ptr<Binlog> logger_;
  std::atomic<bool> binlog_io_error_;

  pthread_rwlock_t db_rwlock_;
  slash::RecordMutex mutex_record_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  /*
   * No allowed copy and copy assign
   */
  Partition(const Partition&);
  void operator=(const Partition&);

};

#endif
