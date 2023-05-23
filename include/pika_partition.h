// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PARTITION_H_
#define PIKA_PARTITION_H_

#include <shared_mutex>

#include "pstd/include/scope_record_lock.h"

#include "storage/backupable.h"
#include "storage/storage.h"

#include "include/pika_binlog.h"

class Cmd;

/*
 *Keyscan used
 */
struct KeyScanInfo {
  time_t start_time = 0;
  std::string s_start_time;
  int32_t duration = -3;
  std::vector<storage::KeyInfo> key_infos;  // the order is strings, hashes, lists, zsets, sets
  bool key_scaning_ = false;
  KeyScanInfo()
      : 
        s_start_time("0"),
        
        key_infos({{0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}, {0, 0, 0, 0}})
        {}
};

struct BgSaveInfo {
  bool bgsaving = false;
  time_t start_time = 0;
  std::string s_start_time;
  std::string path;
  LogOffset offset;
  BgSaveInfo() = default;
  void Clear() {
    bgsaving = false;
    path.clear();
    offset = LogOffset();
  }
};

class Partition : public std::enable_shared_from_this<Partition> {
 public:
  Partition(const std::string& table_name, uint32_t partition_id, const std::string& table_db_path);
  virtual ~Partition();

  std::string GetTableName() const;
  uint32_t GetPartitionId() const;
  std::string GetPartitionName() const;
  std::shared_ptr<storage::Storage> db() const;

  void Compact(const storage::DataType& type);

  void DbRWLockWriter();
  void DbRWLockReader();
  void DbRWUnLock();

  std::shared_ptr<pstd::lock::LockMgr> LockMgr();

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

  // key scan info use
  pstd::Status GetKeyNum(std::vector<storage::KeyInfo>* key_info);
  KeyScanInfo GetKeyScanInfo();

  /*
   * No allowed copy and copy assign
   */
  Partition(const Partition&) = delete;
  void operator=(const Partition&) = delete;

 private:
  std::string table_name_;
  uint32_t partition_id_ = 0;

  std::string db_path_;
  std::string bgsave_sub_path_;
  std::string dbsync_path_;
  std::string partition_name_;

  bool opened_ = false;

  std::shared_mutex db_rwlock_;
  // class may be shared, using shared_ptr would be a better choice
  std::shared_ptr<pstd::lock::LockMgr> lock_mgr_;
  std::shared_ptr<storage::Storage> db_;

  bool full_sync_ = false;

  pstd::Mutex key_info_protector_;
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
  pstd::Mutex bgsave_protector_;
  std::shared_ptr<storage::BackupEngine> bgsave_engine_;

  // key scan info use
  void InitKeyScan();

};

#endif

