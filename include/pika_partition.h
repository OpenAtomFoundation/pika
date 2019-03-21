// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PARTITION_H_
#define PIKA_PARTITION_H_

#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"

#include "include/pika_binlog.h"

class Cmd;

struct BgSaveInfo {
  bool bgsaving;
  time_t start_time;
  std::string s_start_time;
  std::string path;
  uint32_t filenum;
  uint64_t offset;
  BgSaveInfo() : bgsaving(false), filenum(0), offset(0) {}
  void Clear() {
    bgsaving = false;
    path.clear();
    filenum = 0;
    offset = 0;
  }
};

struct BinlogOffset {
  uint32_t filenum;
  uint64_t offset;
  BinlogOffset()
      : filenum(0), offset(0) {}
  BinlogOffset(uint32_t num, uint64_t off)
      : filenum(num), offset(off) {}
};

enum ReplState {
  kNoConnect  = 0,
  kTryConnect = 1,
  kWaitReply  = 2,
  kWaitDBSync = 3,
  kConnected  = 4,
  kError      = 5
};

// debug only
const std::string ReplStateMsg[] = {
  "kNoConnect",
  "kTryConnect",
  "kWaitReply",
  "kConnected",
  "kWaitDBSync",
  "kError"
};

class Partition : public std::enable_shared_from_this<Partition> {
 public:
  Partition(const std::string& table_name,
            uint32_t partition_id,
            const std::string& table_db_path,
            const std::string& table_log_path,
            const std::string& table_trash_path);
  virtual ~Partition();

  std::string GetTableName() const;
  uint32_t GetPartitionId() const;
  std::string GetPartitionName() const;
  std::shared_ptr<Binlog> logger() const;
  std::shared_ptr<blackwidow::BlackWidow> db() const;

  void DoCommand(Cmd* const cmd);
  void Compact(const blackwidow::DataType& type);

  void DbRWLockWriter();
  void DbRWLockReader();
  void DbRWUnLock();
  void RecordLock(const std::string& key);
  void RecordUnLock(const std::string& key);

  void SetBinlogIoError(bool error);
  bool IsBinlogIoError();
  bool GetBinlogOffset(BinlogOffset* const boffset);
  bool SetBinlogOffset(const BinlogOffset& boffset);

  ReplState State();
  void SetReplState(const ReplState& state);

  bool FullSync();
  void SetFullSync(bool full_sync);

  void PrepareRsync();
  bool TryUpdateMasterOffset();
  bool ChangeDb(const std::string& new_path);

  void Leave();
  void Close();
  void MoveToTrash();

  // BgSave use;
  bool IsBgSaving();
  void BgSavePartition();
  BgSaveInfo bgsave_info();

  // Flushall & Flushdb use
  bool FlushAll();
  bool FlushDb(const std::string& db_name);

  // Purgelogs use
  bool PurgeLogs();
  void ClearPurge();

  void RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option) const;


 private:
  std::string table_name_;
  uint32_t partition_id_;

  std::string db_path_;
  std::string log_path_;
  std::string trash_path_;
  std::string bgsave_sub_path_;
  std::string dbsync_path_;
  std::string partition_name_;

  bool opened_;
  std::shared_ptr<Binlog> logger_;
  std::atomic<bool> binlog_io_error_;

  pthread_rwlock_t db_rwlock_;
  slash::RecordMutex mutex_record_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  pthread_rwlock_t state_rwlock_;  // protect partition status below
  ReplState repl_state_;
  bool full_sync_;


  /*
   * BgSave use
   */
  static void DoBgSave(void* arg);
  bool RunBgsaveEngine();
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave();
  void FinishBgsave();
  BgSaveInfo bgsave_info_;
  slash::Mutex bgsave_protector_;
  blackwidow::BackupEngine* bgsave_engine_;

  /*
   * Purgelogs use
   */
  static void DoPurgeLogs(void* arg);
  bool PurgeFiles();
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  std::atomic<bool> purging_;

  /*
   * No allowed copy and copy assign
   */
  Partition(const Partition&);
  void operator=(const Partition&);

};

struct PurgeArg {
  std::shared_ptr<Partition> partition;
  uint32_t to;
  bool manual;
  bool force; // Ignore the delete window
};


#endif
