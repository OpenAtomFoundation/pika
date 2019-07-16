// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_table.h"

#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

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
  log_path_ = TablePath(log_path, "log_" + table_name_);

  slash::CreatePath(db_path_);
  slash::CreatePath(log_path_);

  pthread_rwlock_init(&partitions_rw_, NULL);
}

Table::~Table() {
  StopKeyScan();
  pthread_rwlock_destroy(&partitions_rw_);
  partitions_.clear();
}

std::string Table::GetTableName() {
  return table_name_;
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
    item.second->Compact(type);
  }
}

bool Table::FlushPartitionDB() {
  slash::RWLock rwl(&partitions_rw_, false);
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  for (const auto& item : partitions_) {
    item.second->FlushDB();
  }
  return true;
}

bool Table::FlushPartitionSubDB(const std::string& db_name) {
  slash::RWLock rwl(&partitions_rw_, false);
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  for (const auto& item : partitions_) {
    item.second->FlushSubDB(db_name);
  }
  return true;
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

Status Table::AddPartitions(const std::set<uint32_t>& partition_ids) {
  slash::RWLock l(&partitions_rw_, true);
  for (const uint32_t& id : partition_ids) {
    if (id >= partition_num_) {
      return Status::Corruption("partition index out of range[0, "
              + std::to_string(partition_num_ - 1) + "]");
    } else if (partitions_.find(id) != partitions_.end()) {
      return Status::Corruption("partition "
              + std::to_string(id) + " already exist");
    }
  }

  for (const uint32_t& id : partition_ids) {
    partitions_.emplace(id, std::make_shared<Partition>(
          table_name_, id, db_path_, log_path_));
  }
  return Status::OK();
}

Status Table::RemovePartitions(const std::set<uint32_t>& partition_ids) {
  slash::RWLock l(&partitions_rw_, true);
  for (const uint32_t& id : partition_ids) {
    if (partitions_.find(id) == partitions_.end()) {
      return Status::Corruption("partition " + std::to_string(id) + " not found");
    }
  }

  for (const uint32_t& id : partition_ids) {
    partitions_[id]->Leave();
    partitions_.erase(id);
  }
  return Status::OK();
}

void Table::KeyScan() {
  slash::MutexLock ml(&key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return;
  }

  key_scan_info_.key_scaning_ = true;
  key_scan_info_.duration = -2;       // duration -2 mean the task in waiting status,
                                      // has not been scheduled for exec
  BgTaskArg* bg_task_arg = new BgTaskArg();
  bg_task_arg->table = shared_from_this();
  g_pika_server->KeyScanTaskSchedule(&DoKeyScan, reinterpret_cast<void*>(bg_task_arg));
}

bool Table::IsKeyScaning() {
  slash::MutexLock ml(&key_scan_protector_);
  return key_scan_info_.key_scaning_;
}

void Table::RunKeyScan() {
  Status s;
  std::vector<blackwidow::KeyInfo> new_key_infos(5);

  InitKeyScan();
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    std::vector<blackwidow::KeyInfo> tmp_key_infos;
    s = item.second->GetKeyNum(&tmp_key_infos);
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
  key_scan_info_.duration = time(NULL) - key_scan_info_.start_time;

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
    printf("\n\npartition name : %s\n", item.second->GetPartitionName().c_str());
    item.second->db()->ScanDatabase(type);
  }
}

Status Table::GetPartitionsKeyScanInfo(std::map<uint32_t, KeyScanInfo>* infos) {
  slash::RWLock rwl(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    (*infos)[item.first] = item.second->GetKeyScanInfo();
  }
  return Status::OK();
}

KeyScanInfo Table::GetKeyScanInfo() {
  slash::MutexLock lm(&key_scan_protector_);
  return key_scan_info_;
}

void Table::Compact(const blackwidow::DataType& type) {
  slash::RWLock rwl(&partitions_rw_, true);
  for (const auto& item : partitions_) {
    item.second->Compact(type);
  }
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
  key_scan_info_.duration = -1;       // duration -1 mean the task in processing
}

void Table::LeaveAllPartition() {
  slash::RWLock rwl(&partitions_rw_, true);
  for (const auto& item : partitions_) {
    item.second->Leave();
  }
  partitions_.clear();
}

std::set<uint32_t> Table::GetPartitionIds() {
  std::set<uint32_t> ids;
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : partitions_) {
    ids.insert(item.first);
  }
  return ids;
}

std::shared_ptr<Partition> Table::GetPartitionById(uint32_t partition_id) {
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(partition_id);
  return (iter == partitions_.end()) ? NULL : iter->second;
}

std::shared_ptr<Partition> Table::GetPartitionByKey(const std::string& key) {
  assert(partition_num_ != 0);
  uint32_t index = g_pika_cmd_table_manager->DistributeKey(key, partition_num_);
  slash::RWLock rwl(&partitions_rw_, false);
  auto iter = partitions_.find(index);
  return (iter == partitions_.end()) ? NULL : iter->second;
}
