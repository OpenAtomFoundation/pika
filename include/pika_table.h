// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TABLE_H_
#define PIKA_TABLE_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

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

class Table : public std::enable_shared_from_this<Table>{
 public:
  Table(const std::string& table_name,
        uint32_t partition_num,
        const std::string& db_path,
        const std::string& log_path,
        const std::string& trash_path);
  virtual ~Table();

  friend class Cmd;
  friend class InfoCmd;
  friend class PikaServer;

  std::string GetTableName();
  void BgSaveTable();
  void CompactTable(const blackwidow::DataType& type);
  bool FlushPartitionDB();
  bool FlushPartitionSubDB(const std::string& db_name);
  bool IsBinlogIoError();
  uint32_t PartitionNum();

  // KeyScan use;
  void KeyScan();
  bool IsKeyScaning();
  void RunKeyScan();
  void StopKeyScan();
  void ScanDatabase(const blackwidow::DataType& type);
  KeyScanInfo GetKeyScanInfo();

  // Compact use;
  void Compact(const blackwidow::DataType& type);

  void LeaveAllPartition();
  std::shared_ptr<Partition> GetPartitionById(uint32_t partition_id);
  std::shared_ptr<Partition> GetPartitionByKey(const std::string& key);

 private:
  std::string table_name_;
  uint32_t partition_num_;
  std::string db_path_;
  std::string log_path_;
  std::string trash_path_;

  pthread_rwlock_t partitions_rw_;
  std::map<int32_t, std::shared_ptr<Partition>> partitions_;

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
