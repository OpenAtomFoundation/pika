// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TABLE_H_
#define PIKA_TABLE_H_

#include <string>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "iostream"

#include "include/pika_command.h"
#include "include/pika_partition.h"

static std::unordered_set<std::string> TableMayNotSupportCommands {kCmdNameDel,
                       kCmdNameMget,        kCmdNameKeys,          kCmdNameMset,
                       kCmdNameMsetnx,      kCmdNameScan,          kCmdNameScanx,
                       kCmdNamePKScanRange, kCmdNamePKRScanRange,  kCmdNameRPopLPush,
                       kCmdNameZUnionstore, kCmdNameZInterstore,   kCmdNameSUnion,
                       kCmdNameSUnionstore, kCmdNameSInter,        kCmdNameSInterstore,
                       kCmdNameSDiff,       kCmdNameSDiffstore,    kCmdNameSMove,
                       kCmdNamePfCount,     kCmdNamePfMerge};

class Table {
 public:
  Table(const std::string& table_name,
        uint32_t partition_num,
        const std::string& db_path,
        const std::string& log_path);
  virtual ~Table();

  bool IsCommandSupport(const std::string& cmd) const;
  bool IsBinlogIoError() ;
  uint32_t PartitionNum();

  std::shared_ptr<Partition> GetPartitionById(uint32_t partition_id);
  std::shared_ptr<Partition> GetPartitionByKey(const std::string& key);

 private:
  std::string table_name_;
  uint32_t partition_num_;
  std::string db_path_;
  std::string log_path_;

  pthread_rwlock_t partitions_rw_;
  std::unordered_map<int32_t, std::shared_ptr<Partition>> partitions_;

  /*
   * No allowed copy and copy assign
   */
  Table(const Table&);
  void operator=(const Table&);
};

#endif
