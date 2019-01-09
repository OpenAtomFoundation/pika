// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PARTITION_H_
#define PIKA_PARTITION_H_

#include <string>
#include <memory>
#include <fstream>

#include "iostream"

#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"

#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_binlog.h"

class Cmd;
class Binlog;

struct BGSaveInfo {
  bool bgsaving;
  time_t start_time;
  std::string s_start_time;
  std::string path;
  uint32_t filenum;
  uint64_t offset;
  BGSaveInfo() : bgsaving(false), filenum(0), offset(0){}
  void Clear() {
    bgsaving = false;
    path.clear();
    filenum = 0;
    offset = 0;
  }
};

struct BGSaveArg {
  std::shared_ptr<Partition> partition;
};

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

  // BgSave use;
  void BgSavePartition();
  BGSaveInfo bgsave_info();

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
   * Bgsave use
   */
  static void DoBgSave(void* arg);
  bool RunBgsaveEngine();
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave();
  void FinishBgsave();
  BGSaveInfo bgsave_info_;
  slash::Mutex bgsave_protector_;
  blackwidow::BackupEngine* bgsave_engine_;

  /*
   * No allowed copy and copy assign
   */
  Partition(const Partition&);
  void operator=(const Partition&);

};

#endif
