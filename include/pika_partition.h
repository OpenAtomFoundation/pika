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

/*
 *Keyscan used
 */
struct KeyScanInfo {
  time_t start_time;
  std::string s_start_time;
  int32_t duration;
  std::vector<blackwidow::KeyInfo> key_infos; //the order is strings, hashes, lists, zsets, sets
  bool key_scaning_;
  KeyScanInfo() :
      start_time(0),
      s_start_time("1970-01-01 08:00:00"),
      duration(-3),
      key_infos({{0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}}),
      key_scaning_(false) {
  }
};


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

enum ReplState {
  kNoConnect  = 0,
  kTryConnect = 1,
  kTryDBSync  = 2,
  kWaitDBSync = 3,
  kWaitReply  = 4,
  kConnected  = 5,
  kError      = 6
};

// debug only
const std::string ReplStateMsg[] = {
  "kNoConnect",
  "kTryConnect",
  "kTryDBSync",
  "kWaitDBSync",
  "kWaitReply",
  "kConnected",
  "kError"
};

class Partition : public std::enable_shared_from_this<Partition> {
 public:
  Partition(const std::string& table_name,
            uint32_t partition_id,
            const std::string& table_db_path,
            const std::string& table_log_path);
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

  // FlushDB & FlushSubDB use
  bool FlushDB();
  bool FlushSubDB(const std::string& db_name);

  // Purgelogs use
  bool PurgeLogs(uint32_t to = 0, bool manual = false);
  void ClearPurge();

  // key scan info use
  Status GetKeyNum(std::vector<blackwidow::KeyInfo>* key_info);
  KeyScanInfo GetKeyScanInfo();

 private:
  std::string table_name_;
  uint32_t partition_id_;

  std::string db_path_;
  std::string log_path_;
  std::string bgsave_sub_path_;
  std::string dbsync_path_;
  std::string partition_name_;

  bool opened_;
  std::shared_ptr<Binlog> logger_;
  std::atomic<bool> binlog_io_error_;

  pthread_rwlock_t db_rwlock_;
  slash::RecordMutex mutex_record_;
  std::shared_ptr<blackwidow::BlackWidow> db_;

  bool full_sync_;

  slash::Mutex key_info_protector_;
  KeyScanInfo key_scan_info_;

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
  bool PurgeFiles(uint32_t to, bool manual);
  bool GetBinlogFiles(std::map<uint32_t, std::string>& binlogs);
  std::atomic<bool> purging_;

  // key scan info use
  void InitKeyScan();

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
