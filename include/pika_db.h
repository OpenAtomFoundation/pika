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

class DB : public std::enable_shared_from_this<DB>, public pstd::noncopyable {
 public:
  DB(std::string  db_name, uint32_t slot_num, const std::string& db_path, const std::string& log_path);
  virtual ~DB();

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterInfoCmd;
  friend class PikaServer;

  std::string GetDBName();
  void BgSaveDB();
  void CompactDB(const storage::DataType& type);
  bool FlushSlotDB();
  bool FlushSlotSubDB(const std::string& db_name);
  void SetBinlogIoError();
  bool IsBinlogIoError();
  uint32_t SlotNum();
  void GetAllSlots(std::set<uint32_t>& slot_ids);

  // Dynamic change partition
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

  void LeaveAllSlot();
  std::set<uint32_t> GetSlotIds();
  std::shared_ptr<Slot> GetSlotById(uint32_t slot_id);
  std::shared_ptr<Slot> GetSlotByKey(const std::string& key);
  bool DBIsEmpty();
  pstd::Status MovetoToTrash(const std::string& path);
  pstd::Status Leave();

 private:
  std::string db_name_;
  uint32_t slot_num_ = 0;
  std::string db_path_;
  std::string log_path_;

  std::atomic<bool> binlog_io_error_;
  // lock order
  // slots_rw_ > key_scan_protector_

  std::shared_mutex slots_rw_;
  std::map<uint32_t, std::shared_ptr<Slot>> slots_;

  /*
   * KeyScan use
   */
  static void DoKeyScan(void* arg);
  void InitKeyScan();
  pstd::Mutex key_scan_protector_;
  KeyScanInfo key_scan_info_;
};

struct BgTaskArg {
  std::shared_ptr<DB> db;
  std::shared_ptr<Slot> slot;
};

#endif
