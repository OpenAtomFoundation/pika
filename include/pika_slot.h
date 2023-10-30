// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include <shared_mutex>

#include "pstd/include/scope_record_lock.h"

#include "storage/backupable.h"
#include "storage/storage.h"

#include "include/pika_binlog.h"

class Cmd;
class PikaCache;

/*
 *Keyscan used
 */
struct KeyScanInfo {
  time_t start_time = 0;
  std::string s_start_time;
  int32_t duration = -3;
  std::vector<storage::KeyInfo> key_infos;  // the order is strings, hashes, lists, zsets, sets
  bool key_scaning_ = false;
  KeyScanInfo() :
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

class Slot : public std::enable_shared_from_this<Slot>,public pstd::noncopyable {
 public:
  Slot(const std::string& db_name, uint32_t slot_id, const std::string& table_db_path);
  virtual ~Slot();

  std::string GetDBName() const;
  uint32_t GetSlotID() const;
  std::string GetSlotName() const;
  std::shared_ptr<storage::Storage> db() const;
  std::shared_ptr<PikaCache> cache() const;

  void Init();

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
  void BgSaveSlot();
  BgSaveInfo bgsave_info();
  void GetBgSaveMetaData(std::vector<std::string>* fileNames, std::string* snapshot_uuid);
  pstd::Status GetBgSaveUUID(std::string* snapshot_uuid);

  // FlushDB & FlushSubDB use
  bool FlushDB();
  bool FlushSubDB(const std::string& db_name);

  // key scan info use
  pstd::Status GetKeyNum(std::vector<storage::KeyInfo>* key_info);
  KeyScanInfo GetKeyScanInfo();

  /*
   * SlotsMgrt used
   */
  void GetSlotsMgrtSenderStatus(std::string *ip, int64_t *port, int64_t *slot, bool *migrating, int64_t *moved, int64_t *remained);

 private:
  std::string db_name_;
  uint32_t slot_id_ = 0;
  std::string snapshot_uuid_;

  std::string db_path_;
  std::string bgsave_sub_path_;
  std::string dbsync_path_;
  std::string slot_name_;

  bool opened_ = false;

  std::shared_mutex db_rwlock_;
  // class may be shared, using shared_ptr would be a better choice
  std::shared_ptr<pstd::lock::LockMgr> lock_mgr_;
  std::shared_ptr<storage::Storage> db_;
  std::shared_ptr<PikaCache> cache_;

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

