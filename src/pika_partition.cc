// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_partition.h"

#include <fstream>
#include <memory>

#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

#include "pstd/include/mutex_impl.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

std::string PartitionPath(const std::string& table_path, uint32_t slot_id) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%u/", slot_id);
  return table_path + buf;
}

std::string SlotName(const std::string& table_name, uint32_t slot_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "(%s:%u)", table_name.data(), slot_id);
  return {buf};
}

std::string BgsaveSubPath(const std::string& table_name, uint32_t slot_id) {
  char buf[256];
  std::string slot_id_str = std::to_string(slot_id);
  snprintf(buf, sizeof(buf), "%s/%s", table_name.data(), slot_id_str.data());
  return {buf};
}

std::string DbSyncPath(const std::string& sync_path, const std::string& table_name, const uint32_t slot_id) {
  char buf[256];
  std::string slot_id_str = std::to_string(slot_id);
  snprintf(buf, sizeof(buf), "%s/", table_name.data());
  return sync_path + buf;
}

Slot::Slot(const std::string& table_name, uint32_t slot_id, const std::string& table_db_path)
    : table_name_(table_name), slot_id_(slot_id), bgsave_engine_(nullptr) {
  db_path_ = table_db_path;
  bgsave_sub_path_ = table_name;
  dbsync_path_ = DbSyncPath(g_pika_conf->db_sync_path(), table_name_, slot_id_);
  slot_name_ = table_name;

  db_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = db_->Open(g_pika_server->storage_options(), db_path_);

  lock_mgr_ = std::make_shared<pstd::lock::LockMgr>(1000, 0, std::make_shared<pstd::lock::MutexFactoryImpl>());

  opened_ = s.ok();
  assert(db_);
  assert(s.ok());
  LOG(INFO) << slot_name_ << " DB Success";
}

Slot::~Slot() {
  Close();
}

void Slot::Leave() {
  Close();
  MoveToTrash();
}

void Slot::Close() {
  if (!opened_) {
    return;
  }
  std::lock_guard lock(db_rwlock_);
  db_.reset();
  lock_mgr_.reset();
  opened_ = false;
}

// Before call this function, should
// close db and log first
void Slot::MoveToTrash() {
  if (opened_) {
    return;
  }

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  if (pstd::RenameFile(db_path_, dbpath) != 0) {
    LOG(WARNING) << "Failed to move db to trash, error: " << strerror(errno);
    return;
  }
  g_pika_server->PurgeDir(dbpath);

  LOG(WARNING) << "Slot DB: " << slot_name_ << " move to trash success";
}

std::string Slot::GetTableName() const { return table_name_; }

uint32_t Slot::GetSlotId() const { return slot_id_; }

std::string Slot::GetSlotName() const { return slot_name_; }

std::shared_ptr<storage::Storage> Slot::db() const { return db_; }

void Slot::Compact(const storage::DataType& type) {
  if (!opened_) {
    return;
  }
  db_->Compact(type);
}

void Slot::DbRWLockWriter() { db_rwlock_.lock(); }

void Slot::DbRWLockReader() { db_rwlock_.lock_shared(); }

void Slot::DbRWUnLock() { db_rwlock_.unlock(); }

std::shared_ptr<pstd::lock::LockMgr> Slot::LockMgr() { return lock_mgr_; }

void Slot::PrepareRsync() {
  pstd::DeleteDirIfExist(dbsync_path_);
  pstd::CreatePath(dbsync_path_ + "strings");
  pstd::CreatePath(dbsync_path_ + "hashes");
  pstd::CreatePath(dbsync_path_ + "lists");
  pstd::CreatePath(dbsync_path_ + "sets");
  pstd::CreatePath(dbsync_path_ + "zsets");
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaAuxiliaryThread cron will connect and do slaveof task with master
bool Slot::TryUpdateMasterOffset() {
  std::string info_path = dbsync_path_ + kBgsaveInfoFile;
  if (!pstd::FileExists(info_path)) {
    return false;
  }

  std::shared_ptr<SyncSlaveSlot> slave_slot =
      g_pika_rm->GetSyncSlaveSlotByName(SlotInfo(table_name_, slot_id_));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << slot_name_ << " not exist";
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Slot: " << slot_name_ << ", Failed to open info file after db sync";
    slave_slot->SetReplState(ReplState::kError);
    return false;
  }
  std::string line;
  std::string master_ip;
  int lineno = 0;
  int64_t filenum = 0;
  int64_t offset = 0;
  int64_t term = 0;
  int64_t index = 0;
  int64_t tmp = 0;
  int64_t master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 8) {
      if ((pstd::string2int(line.data(), line.size(), &tmp) == 0) || tmp < 0) {
        LOG(WARNING) << "Slot: " << slot_name_
                     << ", Format of info file after db sync error, line : " << line;
        is.close();
        slave_slot->SetReplState(ReplState::kError);
        return false;
      }
      if (lineno == 3) {
        master_port = tmp;
      } else if (lineno == 4) {
        filenum = tmp;
      } else if (lineno == 5) {
        offset = tmp;
      } else if (lineno == 6) {
        term = tmp;
      } else if (lineno == 7) {
        index = tmp;
      }
    } else if (lineno > 8) {
      LOG(WARNING) << "Slot: " << slot_name_ << ", Format of info file after db sync error, line : " << line;
      is.close();
      slave_slot->SetReplState(ReplState::kError);
      return false;
    }
  }
  is.close();

  LOG(INFO) << "Slot: " << slot_name_ << " Information from dbsync info"
            << ",  master_ip: " << master_ip << ", master_port: " << master_port << ", filenum: " << filenum
            << ", offset: " << offset << ", term: " << term << ", index: " << index;

  // Sanity check
  if (master_ip != slave_slot->MasterIp() || master_port != slave_slot->MasterPort()) {
    LOG(WARNING) << "Slot: " << slot_name_ << " Error master node ip port: " << master_ip << ":"
                 << master_port;
    slave_slot->SetReplState(ReplState::kError);
    return false;
  }

  pstd::DeleteFile(info_path);
  if (!ChangeDb(dbsync_path_)) {
    LOG(WARNING) << "Slot: " << slot_name_ << ", Failed to change db";
    slave_slot->SetReplState(ReplState::kError);
    return false;
  }

  // Update master offset
  std::shared_ptr<SyncMasterSlot> master_slot =
      g_pika_rm->GetSyncMasterSlotByName(SlotInfo(table_name_, slot_id_));
  if (!master_slot) {
    LOG(WARNING) << "Master Slot: " << slot_name_ << " not exist";
    return false;
  }
  if (g_pika_conf->consensus_level() != 0) {
    master_slot->ConsensusReset(LogOffset(BinlogOffset(filenum, offset), LogicOffset(term, index)));
  } else {
    master_slot->Logger()->SetProducerStatus(filenum, offset);
  }
  slave_slot->SetReplState(ReplState::kTryConnect);
  return true;
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool Slot::ChangeDb(const std::string& new_path) {
  std::string tmp_path(db_path_);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  pstd::DeleteDirIfExist(tmp_path);

  std::lock_guard l(db_rwlock_);
  LOG(INFO) << "Slot: " << slot_name_ << ", Prepare change db from: " << tmp_path;
  db_.reset();

  if (0 != pstd::RenameFile(db_path_, tmp_path)) {
    LOG(WARNING) << "Slot: " << slot_name_
                 << ", Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  if (0 != pstd::RenameFile(new_path, db_path_)) {
    LOG(WARNING) << "Slot: " << slot_name_
                 << ", Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }

  db_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = db_->Open(g_pika_server->storage_options(), db_path_);
  assert(db_);
  assert(s.ok());
  pstd::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Slot: " << slot_name_ << ", Change db success";
  return true;
}

bool Slot::IsBgSaving() {
  std::lock_guard ml(bgsave_protector_);
  return bgsave_info_.bgsaving;
}

void Slot::BgSavePartition() {
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return;
  }
  bgsave_info_.bgsaving = true;
  auto bg_task_arg = new BgTaskArg();
  bg_task_arg->slot = shared_from_this();
  g_pika_server->BGSaveTaskSchedule(&DoBgSave, static_cast<void*>(bg_task_arg));
}

BgSaveInfo Slot::bgsave_info() {
  std::lock_guard l(bgsave_protector_);
  return bgsave_info_;
}

void Slot::DoBgSave(void* arg) {
  std::unique_ptr<BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));

  // Do BgSave
  bool success = bg_task_arg->slot->RunBgsaveEngine();

  // Some output
  BgSaveInfo info = bg_task_arg->slot->bgsave_info();
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(nullptr) - info.start_time) << "s\n"
        << g_pika_server->host() << "\n"
        << g_pika_server->port() << "\n"
        << info.offset.b_offset.filenum << "\n"
        << info.offset.b_offset.offset << "\n";
    if (g_pika_conf->consensus_level() != 0) {
      out << info.offset.l_offset.term << "\n" << info.offset.l_offset.index << "\n";
    }
    out.close();
  }
  if (!success) {
    std::string fail_path = info.path + "_FAILED";
    pstd::RenameFile(info.path, fail_path);
  }
  bg_task_arg->slot->FinishBgsave();

}

bool Slot::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << slot_name_ << " after prepare bgsave";

  BgSaveInfo info = bgsave_info();
  LOG(INFO) << slot_name_ << " bgsave_info: path=" << info.path << ",  filenum=" << info.offset.b_offset.filenum
            << ", offset=" << info.offset.b_offset.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);

  if (!s.ok()) {
    LOG(WARNING) << slot_name_ << " create new backup failed :" << s.ToString();
    return false;
  }
  LOG(INFO) << slot_name_ << " create new backup finished.";

  return true;
}

// Prepare engine, need bgsave_protector protect
bool Slot::InitBgsaveEnv() {
  std::lock_guard l(bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(nullptr);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  std::string time_sub_path = g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
  bgsave_info_.path = g_pika_conf->bgsave_path() + time_sub_path + "/" + bgsave_sub_path_;
  if (!pstd::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << slot_name_ << " remove exist bgsave dir failed";
    return false;
  }
  pstd::CreatePath(bgsave_info_.path, 0755);
  // Prepare for failed dir
  if (!pstd::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << slot_name_ << " remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool Slot::InitBgsaveEngine() {
  bgsave_engine_.reset();
  rocksdb::Status s = storage::BackupEngine::Open(db().get(), bgsave_engine_);
  if (!s.ok()) {
    LOG(WARNING) << slot_name_ << " open backup engine failed " << s.ToString();
    return false;
  }

  std::shared_ptr<SyncMasterSlot> slot =
      g_pika_rm->GetSyncMasterSlotByName(SlotInfo(table_name_, slot_id_));
  if (!slot) {
    LOG(WARNING) << slot_name_ << " not found";
    return false;
  }

  {
    std::lock_guard lock(db_rwlock_);
    LogOffset bgsave_offset;
    if (g_pika_conf->consensus_level() != 0) {
      bgsave_offset = slot->ConsensusAppliedIndex();
    } else {
      // term, index are 0
      slot->Logger()->GetProducerStatus(&(bgsave_offset.b_offset.filenum), &(bgsave_offset.b_offset.offset));
    }
    {
      std::lock_guard l(bgsave_protector_);
      bgsave_info_.offset = bgsave_offset;
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()) {
      LOG(WARNING) << slot_name_ << " set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

void Slot::ClearBgsave() {
  std::lock_guard l(bgsave_protector_);
  bgsave_info_.Clear();
}

void Slot::FinishBgsave() {
  std::lock_guard l(bgsave_protector_);
  bgsave_info_.bgsaving = false;
}

bool Slot::FlushDB() {
  std::lock_guard rwl(db_rwlock_);
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << slot_name_ << " Delete old db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  pstd::RenameFile(db_path_, dbpath);

  db_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = db_->Open(g_pika_server->storage_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << slot_name_ << " Open new db success";
  g_pika_server->PurgeDir(dbpath);
  return true;
}

bool Slot::FlushSubDB(const std::string& db_name) {
  std::lock_guard rwl(db_rwlock_);
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << slot_name_ << " Delete old " + db_name + " db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] != '/') {
    dbpath.append("/");
  }

  std::string sub_dbpath = dbpath + db_name;
  std::string del_dbpath = dbpath + db_name + "_deleting";
  pstd::RenameFile(sub_dbpath, del_dbpath);

  db_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = db_->Open(g_pika_server->storage_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << slot_name_ << " open new " + db_name + " db success";
  g_pika_server->PurgeDir(del_dbpath);
  return true;
}

void Slot::InitKeyScan() {
  key_scan_info_.start_time = time(nullptr);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
  key_scan_info_.s_start_time.assign(s_time, len);
  key_scan_info_.duration = -1;  // duration -1 mean the task in processing
}

KeyScanInfo Slot::GetKeyScanInfo() {
  std::lock_guard l(key_info_protector_);
  return key_scan_info_;
}

Status Slot::GetKeyNum(std::vector<storage::KeyInfo>* key_info) {
  std::lock_guard l(key_info_protector_);
  if (key_scan_info_.key_scaning_) {
    *key_info = key_scan_info_.key_infos;
    return Status::OK();
  }
  InitKeyScan();
  key_scan_info_.key_scaning_ = true;
  key_scan_info_.duration = -2;  // duration -2 mean the task in waiting status,
                                 // has not been scheduled for exec
  rocksdb::Status s = db_->GetKeyNum(key_info);
  key_scan_info_.key_scaning_ = false;
  if (!s.ok()) {
    return Status::Corruption(s.ToString());
  }
  key_scan_info_.key_infos = *key_info;
  key_scan_info_.duration = time(nullptr) - key_scan_info_.start_time;
  return Status::OK();
}

