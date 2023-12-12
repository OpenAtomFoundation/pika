// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DB_H_
#define PIKA_DB_H_

#include <shared_mutex>

#include "storage/storage.h"

#include "include/pika_command.h"
#include "include/pika_slot.h"

struct DisplayCacheInfo {
  int status = 0;
  uint32_t cache_num = 0;
  uint64_t keys_num = 0;
  uint64_t used_memory = 0;
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t hits_per_sec = 0;
  uint64_t read_cmd_per_sec = 0;
  double hitratio_per_sec = 0.0;
  double hitratio_all = 0.0;
  uint64_t load_keys_per_sec = 0;
  uint64_t last_time_us = 0;
  uint64_t last_load_keys_num = 0;
  uint32_t waitting_load_keys_num = 0;
  DisplayCacheInfo& operator=(const DisplayCacheInfo &obj) {
    status = obj.status;
    cache_num = obj.cache_num;
    keys_num = obj.keys_num;
    used_memory = obj.used_memory;
    hits = obj.hits;
    misses = obj.misses;
    hits_per_sec = obj.hits_per_sec;
    read_cmd_per_sec = obj.read_cmd_per_sec;
    hitratio_per_sec = obj.hitratio_per_sec;
    hitratio_all = obj.hitratio_all;
    load_keys_per_sec = obj.load_keys_per_sec;
    last_time_us = obj.last_time_us;
    last_load_keys_num = obj.last_load_keys_num;
    waitting_load_keys_num = obj.waitting_load_keys_num;
    return *this;
  }
};

class DB : public std::enable_shared_from_this<DB>, public pstd::noncopyable {
 public:
  DB(std::string  db_name, uint32_t slot_num, const std::string& db_path, const std::string& log_path);
  virtual ~DB();

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterInfoCmd;
  friend class PikaServer;

  std::string GetDBName();
  std::shared_ptr<storage::Storage> storage() const;
  void GetBgSaveMetaData(std::vector<std::string>* fileNames, std::string* snapshot_uuid);
  void BgSaveDB();
  void CompactDB(const storage::DataType& type);
  bool FlushSlotDB();
  bool FlushSlotSubDB(const std::string& db_name);
  void SetBinlogIoError();
  void SetBinlogIoErrorrelieve();
  bool IsBinlogIoError();
  uint32_t SlotNum();
  void GetAllSlots(std::set<uint32_t>& slot_ids);
  std::shared_ptr<PikaCache> cache() const;

  // Dynamic change slot
  pstd::Status AddSlots(const std::set<uint32_t>& slot_ids);
  pstd::Status RemoveSlots(const std::set<uint32_t>& slot_ids);

  // KeyScan use;
  void KeyScan();
  bool IsKeyScaning();
  void RunKeyScan();
  void StopKeyScan();
  void ScanDatabase(const storage::DataType& type);
  KeyScanInfo GetKeyScanInfo();
  pstd::Status GetSlotsKeyScanInfo(std::map<uint32_t, KeyScanInfo>* infos);

  // Compact use;
  void Compact(const storage::DataType& type);
  void CompactRange(const storage::DataType& type, const std::string& start, const std::string& end);

  void LeaveAllSlot();
  std::set<uint32_t> GetSlotIDs();
  std::shared_ptr<Slot> GetSlotById(uint32_t slot_id);
  std::shared_ptr<Slot> GetSlotByKey(const std::string& key);
  bool DBIsEmpty();
  pstd::Status MovetoToTrash(const std::string& path);
  pstd::Status Leave();
  std::map<uint32_t, std::shared_ptr<Slot>> GetSlots() {
    return slots_;
  }
  std::shared_ptr<pstd::lock::LockMgr> LockMgr();
  void DbRWLockWriter();
  void DbRWLockReader();
  void DbRWUnLock();
  /*
   * Cache used
   */
  DisplayCacheInfo GetCacheInfo();
  void UpdateCacheInfo(CacheInfo& cache_info);
  void ResetDisplayCacheInfo(int status);
  uint64_t cache_usage_;
  void Init();

  /*
   * FlushDB & FlushSubDB use
   */
  bool FlushDB();
  bool FlushSubDB(const std::string& db_name);
  bool FlushDBWithoutLock();
  bool FlushSubDBWithoutLock(const std::string& db_name);

  pstd::Status GetBgSaveUUID(std::string* snapshot_uuid);
 private:
  bool opened_ = false;
  std::string db_name_;
  uint32_t slot_num_ = 0;
  std::string db_path_;
  std::string snapshot_uuid_;
  std::string log_path_;
  std::string bgsave_sub_path_;
  pstd::Mutex key_info_protector_;
  std::shared_ptr<pstd::lock::LockMgr> lock_mgr_;
  std::shared_mutex db_rwlock_;
  std::shared_ptr<storage::Storage> storage_;
  std::shared_ptr<PikaCache> cache_;
  std::atomic<bool> binlog_io_error_;
  // lock order
  // slots_rw_ > key_scan_protector_

  std::shared_mutex dbs_rw_;
  std::map<uint32_t, std::shared_ptr<Slot>> slots_;

  /*
   * KeyScan use
   */
  static void DoKeyScan(void* arg);
  void InitKeyScan();
  pstd::Mutex key_scan_protector_;
  KeyScanInfo key_scan_info_;
  /*
   * Cache used
   */
  DisplayCacheInfo cache_info_;
  std::shared_mutex cache_info_rwlock_;
  /*
   * BgSave use
   */
  static void DoBgSave(void* arg);
  bool RunBgsaveEngine();
  BgSaveInfo bgsave_info();
  bool InitBgsaveEnv();
  bool InitBgsaveEngine();
  void ClearBgsave();
  void FinishBgsave();
  BgSaveInfo bgsave_info_;
  pstd::Mutex bgsave_protector_;
  std::shared_ptr<storage::BackupEngine> bgsave_engine_;
};

struct BgTaskArg {
  std::shared_ptr<DB> db;
};

#endif
