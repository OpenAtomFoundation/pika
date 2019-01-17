// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_table.h"

extern PikaServer* g_pika_server;

std::string TablePath(const std::string& path,
                      const std::string& table_name) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", table_name.data());
  return path + buf;
}

Table::Table(const std::string& table_name,
             uint32_t partition_num,
             const std::string& db_path,
             const std::string& log_path) :
  table_name_(table_name),
  partition_num_(partition_num) {

  db_path_ = TablePath(db_path, table_name_);
  log_path_ = TablePath(log_path, table_name_);

  slash::CreatePath(db_path_);
  slash::CreatePath(log_path_);

  pthread_rwlock_init(&partitions_rw_, NULL);

  for (uint32_t idx = 0; idx < partition_num_; ++idx) {
    partitions_.emplace(idx, std::shared_ptr<Partition>(
                new Partition(table_name_, idx, db_path_, log_path_)));
  }

}

Table::~Table() {
  StopKeyScan();
  pthread_rwlock_destroy(&partitions_rw_);
  partitions_.clear();
}

void Table::BgSaveTable() {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->BgSavePartition();
  }
}

void Table::CompactTable(const blackwidow::DataType& type) {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->db()->Compact(type);
  }
}

bool Table::FlushAllTable() {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->FlushAll();
  }
  return true;
}

bool Table::FlushDbTable(const std::string& db_name) {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    item.second->FlushDb(db_name);
  }
  return true;
}

bool Table::IsCommandSupport(const std::string& cmd) const {
  if (partition_num_ == 1) {
    return true;
  } else {
    std::string command = cmd;
    slash::StringToLower(command);
    auto iter = TableMayNotSupportCommands.find(command);
    return (iter == TableMayNotSupportCommands.end()) ? true : false;
  }
}

bool Table::IsBinlogIoError() {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    if (item.second->IsBinlogIoError()) {
      return true;
    }
  }
  return false;
}

uint32_t Table::PartitionNum() {
  return partition_num_;
}

bool Table::key_scaning() {
  slash::MutexLock ml(&key_scan_protector_);
  return key_scan_info_.key_scaning_;
}

void Table::KeyScan() {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return;
  }

  key_scan_info_.key_scaning_ = true;
  InitKeyScan();
  BgTaskArg* bg_task_arg = new BgTaskArg();
  bg_task_arg->table = shared_from_this();
  g_pika_server->KeyScanTaskSchedule(&DoKeyScan, reinterpret_cast<void*>(bg_task_arg));
}

void Table::RunKeyScan() {
  rocksdb::Status s;
  std::vector<blackwidow::KeyInfo> new_key_infos(5);
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    std::vector<blackwidow::KeyInfo> tmp_key_infos;
    s = item.second->db()->GetKeyNum(&tmp_key_infos);
    if (s.ok()) {
      for (size_t idx = 0; idx < tmp_key_infos.size(); ++idx) {
        new_key_infos[idx].keys += tmp_key_infos[idx].keys;
        new_key_infos[idx].expires += tmp_key_infos[idx].expires;
        new_key_infos[idx].avg_ttl += tmp_key_infos[idx].avg_ttl;
        new_key_infos[idx].invaild_keys += tmp_key_infos[idx].invaild_keys;
      }
    } else {
      break;
    }
  }

  slash::MutexLock lm(&key_scan_protector_);
  if (s.ok()) {
    key_scan_info_.key_infos = new_key_infos;
  }
  key_scan_info_.key_scaning_ = false;
}

void Table::StopKeyScan() {
  slash::RWLock rwl(&partitions_rw_, false);
  slash::MutexLock ml(&key_scan_protector_);
  for (const auto& item : partitions_) {
    item.second->db()->StopScanKeyNum();
  }
  key_scan_info_.key_scaning_ = false;
}

void Table::ScanDatabase(const blackwidow::DataType& type) {
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    printf("\n\npartition name : %s\n", item.second->partition_name().c_str());
    item.second->db()->ScanDatabase(type);
  }
}

KeyScanInfo Table::key_scan_info() {
  slash::MutexLock lm(&key_scan_protector_);
  return key_scan_info_;
}

void Table::DoKeyScan(void *arg) {
  BgTaskArg* bg_task_arg = reinterpret_cast<BgTaskArg*>(arg);
  bg_task_arg->table->RunKeyScan();
  delete bg_task_arg;
}

void Table::InitKeyScan() {
  key_scan_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
}

std::shared_ptr<Partition> Table::GetPartitionById(uint32_t partition_id) {
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(partition_id);
  return (iter == partitions_.end()) ? NULL : iter->second;
}

std::shared_ptr<Partition> Table::GetPartitionByKey(const std::string& key) {
  assert(partition_num_ != 0);
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(std::hash<std::string>()(key) % partition_num_);
  return (iter == partitions_.end()) ? NULL : iter->second;
}
