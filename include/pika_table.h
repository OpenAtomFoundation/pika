// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TABLE_H_
#define PIKA_TABLE_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

class Table : public std::enable_shared_from_this<Table>{
 public:
  Table(const std::string& table_name,
        uint32_t partition_num,
        const std::string& db_path,
        const std::string& log_path);
  virtual ~Table();

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterInfoCmd;
  friend class PikaServer;

  std::string GetTableName();
  void BgSaveTable();
  void CompactTable(const blackwidow::DataType& type);
  bool FlushPartitionDB();
  bool FlushPartitionSubDB(const std::string& db_name);
  bool IsBinlogIoError();
  uint32_t PartitionNum();
  void GetAllPartitions(std::set<uint32_t>& partition_ids);

  // Dynamic change partition
  Status AddPartitions(const std::set<uint32_t>& partition_ids);
  Status RemovePartitions(const std::set<uint32_t>& partition_ids);

  // KeyScan use;
  void KeyScan();
  bool IsKeyScaning();
  void RunKeyScan();
  void StopKeyScan();
  void ScanDatabase(const blackwidow::DataType& type);
  KeyScanInfo GetKeyScanInfo();
  Status GetPartitionsKeyScanInfo(std::map<uint32_t, KeyScanInfo>* infos);

  // Compact use;
  void Compact(const blackwidow::DataType& type);

  void LeaveAllPartition();
  std::set<uint32_t> GetPartitionIds();
  std::shared_ptr<Partition> GetPartitionById(uint32_t partition_id);
  std::shared_ptr<Partition> GetPartitionByKey(const std::string& key);
  bool TableIsEmpty();
  Status MovetoToTrash(const std::string& path);
  Status Leave();

 private:
  std::string table_name_;
  uint32_t partition_num_;
  std::string db_path_;
  std::string log_path_;

  // lock order
  // partitions_rw_ > key_scan_protector_

  pthread_rwlock_t partitions_rw_;
  std::map<uint32_t, std::shared_ptr<Partition>> partitions_;

  /*
   * KeyScan use
   */
  static void DoKeyScan(void *arg);
  void InitKeyScan();
  slash::Mutex key_scan_protector_;
  KeyScanInfo key_scan_info_;

  /*
   * No allowed copy and copy assign
   */
  Table(const Table&);
  void operator=(const Table&);
};

struct BgTaskArg {
  std::shared_ptr<Table> table;
  std::shared_ptr<Partition> partition;
};


#endif
