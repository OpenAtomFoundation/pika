// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <utility>

#include "include/pika_db.h"

#include "include/pika_cmd_table_manager.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

std::string DBPath(const std::string& path, const std::string& db_name) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%s/", db_name.data());
  return path + buf;
}

DB::DB(std::string  db_name, uint32_t slot_num, const std::string& db_path,
             const std::string& log_path)
    : db_name_(std::move(db_name)), slot_num_(slot_num) {
  db_path_ = DBPath(db_path, db_name_);
  log_path_ = DBPath(log_path, "log_" + db_name_);

  pstd::CreatePath(db_path_);
  pstd::CreatePath(log_path_);

  binlog_io_error_.store(false);
}

DB::~DB() {
  StopKeyScan();
  slots_.clear();
}

std::string DB::GetDBName() { return db_name_; }

void DB::BgSaveDB() {
  std::shared_lock l(slots_rw_);
  for (const auto& item : slots_) {
    item.second->BgSaveSlot();
  }
}

void DB::CompactDB(const storage::DataType& type) {
  std::shared_lock l(slots_rw_);
  for (const auto& item : slots_) {
    item.second->Compact(type);
  }
}

bool DB::FlushSlotDB() {
  std::shared_lock l(slots_rw_);
  std::lock_guard ml(key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  for (const auto& item : slots_) {
    item.second->FlushDB();
  }
  return true;
}

bool DB::FlushSlotSubDB(const std::string& db_name) {
  std::shared_lock l(slots_rw_);
  std::lock_guard ml(key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return false;
  }
  for (const auto& item : slots_) {
    item.second->FlushSubDB(db_name);
  }
  return true;
}

void DB::SetBinlogIoError() { return binlog_io_error_.store(true); }

bool DB::IsBinlogIoError() { return binlog_io_error_.load(); }

uint32_t DB::SlotNum() { return slot_num_; }

Status DB::AddSlots(const std::set<uint32_t>& slot_ids) {
  std::lock_guard l(slots_rw_);
  for (const uint32_t& id : slot_ids) {
    if (id >= slot_num_) {
      return Status::Corruption("slot index out of range[0, " + std::to_string(slot_num_ - 1) + "]");
    } else if (slots_.find(id) != slots_.end()) {
      return Status::Corruption("slot " + std::to_string(id) + " already exist");
    }
  }

  for (const uint32_t& id : slot_ids) {
    slots_.emplace(id, std::make_shared<Slot>(db_name_, id, db_path_));
  }
  return Status::OK();
}

Status DB::RemoveSlots(const std::set<uint32_t>& slot_ids) {
  std::lock_guard l(slots_rw_);
  for (const uint32_t& id : slot_ids) {
    if (slots_.find(id) == slots_.end()) {
      return Status::Corruption("slot " + std::to_string(id) + " not found");
    }
  }

  for (const uint32_t& id : slot_ids) {
    slots_[id]->Leave();
    slots_.erase(id);
  }
  return Status::OK();
}

void DB::GetAllSlots(std::set<uint32_t>& slot_ids) {
  std::shared_lock l(slots_rw_);
  for (const auto& iter : slots_) {
    slot_ids.insert(iter.first);
  }
}

void DB::KeyScan() {
  std::lock_guard ml(key_scan_protector_);
  if (key_scan_info_.key_scaning_) {
    return;
  }

  key_scan_info_.key_scaning_ = true;
  key_scan_info_.duration = -2;  // duration -2 mean the task in waiting status,
                                 // has not been scheduled for exec
  auto bg_task_arg = new BgTaskArg();
  bg_task_arg->db = shared_from_this();
  g_pika_server->KeyScanTaskSchedule(&DoKeyScan, reinterpret_cast<void*>(bg_task_arg));
}

bool DB::IsKeyScaning() {
  std::lock_guard ml(key_scan_protector_);
  return key_scan_info_.key_scaning_;
}

void DB::RunKeyScan() {
  Status s;
  std::vector<storage::KeyInfo> new_key_infos(5);

  InitKeyScan();
  std::shared_lock l(slots_rw_);
  for (const auto& item : slots_) {
    std::vector<storage::KeyInfo> tmp_key_infos;
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
  key_scan_info_.duration = time(nullptr) - key_scan_info_.start_time;

  std::lock_guard lm(key_scan_protector_);
  if (s.ok()) {
    key_scan_info_.key_infos = new_key_infos;
  }
  key_scan_info_.key_scaning_ = false;
}

void DB::StopKeyScan() {
  std::shared_lock rwl(slots_rw_);
  std::lock_guard ml(key_scan_protector_);

  if (!key_scan_info_.key_scaning_) {
    return;
  }
  for (const auto& item : slots_) {
    item.second->db()->StopScanKeyNum();
  }
  key_scan_info_.key_scaning_ = false;
}

void DB::ScanDatabase(const storage::DataType& type) {
  std::shared_lock l(slots_rw_);
  for (const auto& item : slots_) {
    printf("\n\nslot name : %s\n", item.second->GetSlotName().c_str());
    item.second->db()->ScanDatabase(type);
  }
}

Status DB::GetSlotsKeyScanInfo(std::map<uint32_t, KeyScanInfo>* infos) {
  std::shared_lock l(slots_rw_);
  for (const auto& [id, slot] : slots_) {
    (*infos)[id] = slot->GetKeyScanInfo();
  }
  return Status::OK();
}

KeyScanInfo DB::GetKeyScanInfo() {
  std::lock_guard lm(key_scan_protector_);
  return key_scan_info_;
}

void DB::Compact(const storage::DataType& type) {
  std::lock_guard rwl(slots_rw_);
  for (const auto& item : slots_) {
    item.second->Compact(type);
  }
}

void DB::DoKeyScan(void* arg) {
  std::unique_ptr <BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));
  bg_task_arg->db->RunKeyScan();
}

void DB::InitKeyScan() {
  key_scan_info_.start_time = time(nullptr);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
  key_scan_info_.duration = -1;  // duration -1 mean the task in processing
}

void DB::LeaveAllSlot() {
  std::lock_guard l(slots_rw_);
  for (const auto& item : slots_) {
    item.second->Leave();
  }
  slots_.clear();
}

std::set<uint32_t> DB::GetSlotIDs() {
  std::set<uint32_t> ids;
  std::shared_lock l(slots_rw_);
  for (const auto& item : slots_) {
    ids.insert(item.first);
  }
  return ids;
}

std::shared_ptr<Slot> DB::GetSlotById(uint32_t slot_id) {
  std::shared_lock l(slots_rw_);
  auto iter = slots_.find(slot_id);
  return (iter == slots_.end()) ? nullptr : iter->second;
}

std::shared_ptr<Slot> DB::GetSlotByKey(const std::string& key) {
  assert(slot_num_ != 0);
  uint32_t index = g_pika_cmd_table_manager->DistributeKey(key, slot_num_);
  std::shared_lock l(slots_rw_);
  auto iter = slots_.find(index);
  return (iter == slots_.end()) ? nullptr : iter->second;
}

bool DB::DBIsEmpty() {
  std::shared_lock l(slots_rw_);
  return slots_.empty();
}

Status DB::Leave() {
  if (!DBIsEmpty()) {
    return Status::Corruption("DB have slots!");
  }
  return MovetoToTrash(db_path_);
}

Status DB::MovetoToTrash(const std::string& path) {
  std::string path_tmp = path;
  if (path_tmp[path_tmp.length() - 1] == '/') {
    path_tmp.erase(path_tmp.length() - 1);
  }
  path_tmp += "_deleting/";
  if (pstd::RenameFile(path, path_tmp) != 0) {
    LOG(WARNING) << "Failed to move " << path << " to trash, error: " << strerror(errno);
    return Status::Corruption("Failed to move %s to trash", path);
  }
  g_pika_server->PurgeDir(path_tmp);
  LOG(WARNING) << path << " move to trash success";
  return Status::OK();
}
