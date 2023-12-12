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
  std::shared_lock l(dbs_rw_);
  std::lock_guard ml(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return;
  }
  bgsave_info_.bgsaving = true;
  auto bg_task_arg = new BgTaskArg();
  bg_task_arg->db = shared_from_this();
  g_pika_server->BGSaveTaskSchedule(&DoBgSave, static_cast<void*>(bg_task_arg));
}

void DB::SetBinlogIoError() { return binlog_io_error_.store(true); }
void DB::SetBinlogIoErrorrelieve() { return binlog_io_error_.store(false); }
bool DB::IsBinlogIoError() { return binlog_io_error_.load(); }
std::shared_ptr<pstd::lock::LockMgr> Slot::LockMgr() { return lock_mgr_; }
void DB::DbRWLockReader() { db_rwlock_.lock_shared(); }
void DB::DbRWUnLock() { db_rwlock_.unlock(); }
std::shared_ptr<PikaCache> DB::cache() const { return cache_; }
std::shared_ptr<storage::Storage> DB::storage() const { return storage_; }


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
    slots_[id]->Init();

  }
  return Status::OK();
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
  key_scan_info_.duration = static_cast<int32_t>(time(nullptr) - key_scan_info_.start_time);

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

void DB::CompactRange(const storage::DataType& type, const std::string& start, const std::string& end) {
  std::lock_guard rwl(slots_rw_);
  for (const auto& item : slots_) {
    item.second->CompactRange(type, start, end);
  }
}

void DB::DoKeyScan(void* arg) {
  std::unique_ptr <BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));
  bg_task_arg->db->RunKeyScan();
}

void DB::InitKeyScan() {
  key_scan_info_.start_time = time(nullptr);
  char s_time[32];
  size_t len = strftime(s_time, sizeof(s_time), "%Y-%m-%d %H:%M:%S", localtime(&key_scan_info_.start_time));
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

void DB::DbRWLockWriter() { db_rwlock_.lock(); }

DisplayCacheInfo DB::GetCacheInfo() {
  std::lock_guard l(key_info_protector_);
  return cache_info_;
}

bool DB::FlushDBWithoutLock() {
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << db_name_ << " Delete old db...";
  storage_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  pstd::RenameFile(db_path_, dbpath);

  storage_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = storage_->Open(g_pika_server->storage_options(), db_path_);
  assert(storage_);
  assert(s.ok());
  LOG(INFO) << db_name_ << " Open new db success";
  g_pika_server->PurgeDir(dbpath);
  return true;
}

bool DB::FlushSubDBWithoutLock(const std::string& db_name) {
  std::lock_guard l(bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << db_name_ << " Delete old " + db_name + " db...";
  storage_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] != '/') {
    dbpath.append("/");
  }

  std::string sub_dbpath = dbpath + db_name;
  std::string del_dbpath = dbpath + db_name + "_deleting";
  pstd::RenameFile(sub_dbpath, del_dbpath);

  storage_ = std::make_shared<storage::Storage>();
  rocksdb::Status s = storage_->Open(g_pika_server->storage_options(), db_path_);
  assert(storage_);
  assert(s.ok());
  LOG(INFO) << db_name_ << " open new " + db_name + " db success";
  g_pika_server->PurgeDir(del_dbpath);
  return true;
}

void DB::DoBgSave(void* arg) {
  std::unique_ptr<BgTaskArg> bg_task_arg(static_cast<BgTaskArg*>(arg));

  // Do BgSave
  bool success = bg_task_arg->db->RunBgsaveEngine();

  // Some output
  BgSaveInfo info = bg_task_arg->db->bgsave_info();
  std::stringstream info_content;
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    info_content << (time(nullptr) - info.start_time) << "s\n"
                 << g_pika_server->host() << "\n"
                 << g_pika_server->port() << "\n"
                 << info.offset.b_offset.filenum << "\n"
                 << info.offset.b_offset.offset << "\n";
    bg_task_arg->db->snapshot_uuid_ = md5(info_content.str());
    out << info_content.rdbuf();
    out.close();
  }
  if (!success) {
    std::string fail_path = info.path + "_FAILED";
    pstd::RenameFile(info.path, fail_path);
  }
  bg_task_arg->db->FinishBgsave();
}

bool DB::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << db_name_ << " after prepare bgsave";

  BgSaveInfo info = bgsave_info();
  LOG(INFO) << db_name_ << " bgsave_info: path=" << info.path << ",  filenum=" << info.offset.b_offset.filenum
            << ", offset=" << info.offset.b_offset.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);

  if (!s.ok()) {
    LOG(WARNING) << db_name_ << " create new backup failed :" << s.ToString();
    return false;
  }
  LOG(INFO) << db_name_ << " create new backup finished.";

  return true;
}

BgSaveInfo DB::bgsave_info() {
  std::lock_guard l(bgsave_protector_);
  return bgsave_info_;
}

void DB::FinishBgsave() {
  std::lock_guard l(bgsave_protector_);
  bgsave_info_.bgsaving = false;
}

// Prepare engine, need bgsave_protector protect
bool DB::InitBgsaveEnv() {
  std::lock_guard l(bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(nullptr);
  char s_time[32];
  int len = static_cast<int32_t>(strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time)));
  bgsave_info_.s_start_time.assign(s_time, len);
  std::string time_sub_path = g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
  bgsave_info_.path = g_pika_conf->bgsave_path() + time_sub_path + "/" + bgsave_sub_path_;
  if (!pstd::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << db_name_ << " remove exist bgsave dir failed";
    return false;
  }
  pstd::CreatePath(bgsave_info_.path, 0755);
  // Prepare for failed dir
  if (!pstd::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << db_name_ << " remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool DB::InitBgsaveEngine() {
  bgsave_engine_.reset();
  rocksdb::Status s = storage::BackupEngine::Open(storage().get(), bgsave_engine_);
  if (!s.ok()) {
    LOG(WARNING) << db_name_ << " open backup engine failed " << s.ToString();
    return false;
  }

  std::shared_ptr<SyncMasterDB> db =
      g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name_));
  if (!db) {
    LOG(WARNING) << db_name_ << " not found";
    return false;
  }

  {
    std::lock_guard lock(db_rwlock_);
    LogOffset bgsave_offset;
    // term, index are 0
    db->Logger()->GetProducerStatus(&(bgsave_offset.b_offset.filenum), &(bgsave_offset.b_offset.offset));
    {
      std::lock_guard l(bgsave_protector_);
      bgsave_info_.offset = bgsave_offset;
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()) {
      LOG(WARNING) << db_name_ << " set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

void DB::Init() {
  cache_ = std::make_shared<PikaCache>(g_pika_conf->zset_cache_start_pos(), g_pika_conf->zset_cache_field_num_per_key());
  // Create cache
  cache::CacheConfig cache_cfg;
  g_pika_server->CacheConfigInit(cache_cfg);
  cache_->Init(g_pika_conf->GetCacheNum(), &cache_cfg);
}

void DB::GetBgSaveMetaData(std::vector<std::string>* fileNames, std::string* snapshot_uuid) {
  const std::string slotPath = bgsave_info().path;

  std::string types[] = {storage::STRINGS_DB, storage::HASHES_DB, storage::LISTS_DB, storage::ZSETS_DB, storage::SETS_DB};
  for (const auto& type : types) {
    std::string typePath = slotPath + ((slotPath.back() != '/') ? "/" : "") + type;
    if (!pstd::FileExists(typePath)) {
      continue ;
    }

    std::vector<std::string> tmpFileNames;
    int ret = pstd::GetChildren(typePath, tmpFileNames);
    if (ret) {
      LOG(WARNING) << slotPath << " read dump meta files failed, path " << typePath;
      return;
    }

    for (const std::string fileName : tmpFileNames) {
      fileNames -> push_back(type + "/" + fileName);
    }
  }
  fileNames->push_back(kBgsaveInfoFile);
  pstd::Status s = GetBgSaveUUID(snapshot_uuid);
  if (!s.ok()) {
    LOG(WARNING) << "read dump meta info failed! error:" << s.ToString();
    return;
  }
}

Status DB::GetBgSaveUUID(std::string* snapshot_uuid) {
  if (snapshot_uuid_.empty()) {
    std::string info_data;
    const std::string infoPath = bgsave_info().path + "/info";
    //TODO: using file read function to replace rocksdb::ReadFileToString
    rocksdb::Status s = rocksdb::ReadFileToString(rocksdb::Env::Default(), infoPath, &info_data);
    if (!s.ok()) {
      LOG(WARNING) << "read dump meta info failed! error:" << s.ToString();
      return Status::IOError("read dump meta info failed", infoPath);
    }
    pstd::MD5 md5 = pstd::MD5(info_data);
    snapshot_uuid_ = md5.hexdigest();
  }
  *snapshot_uuid = snapshot_uuid_;
  return Status::OK();
}