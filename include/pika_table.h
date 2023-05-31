// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TABLE_H_
#define PIKA_TABLE_H_

#include <shared_mutex>

#include "storage/storage.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

class Table : public std::enable_shared_from_this<Table>, public pstd::noncopyable {
 public:
  Table(std::string  table_name, uint32_t slot_num, const std::string& db_path, const std::string& log_path);
  virtual ~Table();

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterInfoCmd;
  friend class PikaServer;

  std::string GetTableName();
  void BgSaveTable();
  void CompactTable(const storage::DataType& type);
  bool FlushPartitionDB();
  bool FlushPartitionSubDB(const std::string& db_name);
  void SetBinlogIoError();
  bool IsBinlogIoError();
  uint32_t SlotNum();
  void GetAllPartitions(std::set<uint32_t>& slot_ids);

  // Dynamic change partition
  pstd::Status AddPartitions(const std::set<uint32_t>& slot_ids);
  pstd::Status RemovePartitions(const std::set<uint32_t>& slot_ids);

  // KeyScan use;
  void KeyScan();
  bool IsKeyScaning();
  void RunKeyScan();
  void StopKeyScan();
  void ScanDatabase(const storage::DataType& type);
  KeyScanInfo GetKeyScanInfo();
  pstd::Status GetPartitionsKeyScanInfo(std::map<uint32_t, KeyScanInfo>* infos);

  // Compact use;
  void Compact(const storage::DataType& type);

  void LeaveAllPartition();
  std::set<uint32_t> GetSlotIds();
  std::shared_ptr<Slot> GetSlotById(uint32_t slot_id);
  std::shared_ptr<Slot> GetSlotByKey(const std::string& key);
  bool TableIsEmpty();
  pstd::Status MovetoToTrash(const std::string& path);
  pstd::Status Leave();

 private:
  std::string table_name_;
  uint32_t slot_num_ = 0;
  std::string db_path_;
  std::string log_path_;

  std::atomic<bool> binlog_io_error_;
  // lock order
  // partitions_rw_ > key_scan_protector_

  std::shared_mutex partitions_rw_;
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
  std::shared_ptr<Table> table;
  std::shared_ptr<Slot> slot;
};

#endif
