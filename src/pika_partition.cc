// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_partition.h"

#include <fstream>

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_rm.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

std::string PartitionPath(const std::string& table_path,
                          uint32_t partition_id) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%u/", partition_id);
  return table_path + buf;
}

std::string PartitionName(const std::string& table_name,
                          uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "(%s|%u)", table_name.data(), partition_id);
  return std::string(buf);
}

std::string BgsaveSubPath(const std::string& table_name,
                          uint32_t partition_id) {
  char buf[256];
  std::string partition_id_str = std::to_string(partition_id);
  snprintf(buf, sizeof(buf), "%s/%s", table_name.data(), partition_id_str.data());
  return std::string(buf);
}

std::string DbSyncPath(const std::string& sync_path,
                       const std::string& table_name,
                       const uint32_t partition_id,
                       bool classic_mode) {
  char buf[256];
  std::string partition_id_str = std::to_string(partition_id);
  if (classic_mode) {
    snprintf(buf, sizeof(buf), "%s/", table_name.data());
  } else {
    snprintf(buf, sizeof(buf), "%s/%s/", table_name.data(), partition_id_str.data());
  }
  return sync_path + buf;
}

Partition::Partition(const std::string& table_name,
                     uint32_t partition_id,
                     const std::string& table_db_path,
                     const std::string& table_log_path) :
  table_name_(table_name),
  partition_id_(partition_id),
  binlog_io_error_(false),
  bgsave_engine_(NULL),
  purging_(false) {

  db_path_ = g_pika_conf->classic_mode() ?
      table_db_path : PartitionPath(table_db_path, partition_id_);
  log_path_ = g_pika_conf->classic_mode() ?
      table_log_path : PartitionPath(table_log_path, partition_id_);
  bgsave_sub_path_ = g_pika_conf->classic_mode() ?
      table_name : BgsaveSubPath(table_name_, partition_id_);
  dbsync_path_ = DbSyncPath(g_pika_conf->db_sync_path(), table_name_,
          partition_id_,  g_pika_conf->classic_mode());
  partition_name_ = g_pika_conf->classic_mode() ?
      table_name : PartitionName(table_name_, partition_id_);

  pthread_rwlockattr_t attr;
  pthread_rwlockattr_init(&attr);
  pthread_rwlockattr_setkind_np(&attr,
          PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);

  pthread_rwlock_init(&db_rwlock_, &attr);

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);

  opened_ = s.ok() ? true : false;
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " DB Success";

  logger_ = std::shared_ptr<Binlog>(
          new Binlog(log_path_, g_pika_conf->binlog_file_size()));
}

Partition::~Partition() {
  Close();
  delete bgsave_engine_;
  pthread_rwlock_destroy(&db_rwlock_);
}

void Partition::Leave() {
  Close();
  MoveToTrash();
}

void Partition::Close() {
  if (!opened_) {
    return;
  }
  slash::RWLock rwl(&db_rwlock_, true);
  db_.reset();
  logger_.reset();
  opened_ = false;
}

// Before call this function, should
// close db and log first
void Partition::MoveToTrash() {
  if (opened_) {
    return;
  }

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  if (slash::RenameFile(db_path_, dbpath.c_str())) {
    LOG(WARNING) << "Failed to move db to trash, error: " << strerror(errno);
    return;
  }
  g_pika_server->PurgeDir(dbpath);

  std::string logpath = log_path_;
  if (logpath[logpath.length() - 1] == '/') {
    logpath.erase(logpath.length() - 1);
  }
  logpath.append("_deleting/");
  if (slash::RenameFile(log_path_, logpath.c_str())) {
    LOG(WARNING) << "Failed to move log to trash, error: " << strerror(errno);
    return;
  }
  g_pika_server->PurgeDir(logpath);

  LOG(WARNING) << "Partition: " << partition_name_ << " move to trash success";
}

std::string Partition::GetTableName() const {
  return table_name_;
}

uint32_t Partition::GetPartitionId() const {
  return partition_id_;
}

std::string Partition::GetPartitionName() const {
  return partition_name_;
}

std::shared_ptr<Binlog> Partition::logger() const {
  return logger_;
}

std::shared_ptr<blackwidow::BlackWidow> Partition::db() const {
  return db_;
}

void Partition::DoCommand(Cmd* const cmd) {
  if (!opened_) {
    LOG(WARNING) << partition_name_ << " not opened, failed to exec command";
    cmd->res().SetRes(CmdRes::kErrOther, "Partition Not Opened");
    return;
  }

  if (!cmd->is_suspend()) {
    DbRWLockReader();
  }
  if (cmd->is_write()) {
    RecordLock(cmd->current_key());
  }

  uint32_t exec_time = time(nullptr);
  cmd->Do(shared_from_this());

  if (cmd->res().ok()
    && cmd->is_write()
    && g_pika_conf->write_binlog()) {

    logger_->Lock();
    uint32_t filenum = 0;
    uint64_t offset = 0;
    uint64_t logic_id = 0;
    logger_->GetProducerStatus(&filenum, &offset, &logic_id);

    std::string binlog = cmd->ToBinlog(exec_time,
                                       g_pika_conf->server_id(),
                                       logic_id,
                                       filenum,
                                       offset);
    slash::Status s;
    if (!binlog.empty()) {
      s = logger_->Put(binlog);
    }

    logger_->Unlock();
    if (!s.ok()) {
      LOG(WARNING) << partition_name_ << " Writing binlog failed, maybe no space left on device";
      SetBinlogIoError(true);
      cmd->res().SetRes(CmdRes::kErrOther, "Writing binlog failed, maybe no space left on device");
    }
  }

  if (cmd->is_write()) {
    RecordUnLock(cmd->current_key());
  }
  if (!cmd->is_suspend()) {
    DbRWUnLock();
  }
}

void Partition::Compact(const blackwidow::DataType& type) {
  if (!opened_) return;
  db_->Compact(type);
}

void Partition::DbRWLockWriter() {
  pthread_rwlock_wrlock(&db_rwlock_);
}

void Partition::DbRWLockReader() {
  pthread_rwlock_rdlock(&db_rwlock_);
}

void Partition::DbRWUnLock() {
  pthread_rwlock_unlock(&db_rwlock_);
}

void Partition::RecordLock(const std::string& key) {
  mutex_record_.Lock(key);
}

void Partition::RecordUnLock(const std::string& key) {
  mutex_record_.Unlock(key);
}

void Partition::SetBinlogIoError(bool error) {
  binlog_io_error_ = error;
}

bool Partition::IsBinlogIoError() {
  return binlog_io_error_;
}

bool Partition::GetBinlogOffset(BinlogOffset* const boffset) {
  if (opened_) {
    logger_->GetProducerStatus(&boffset->filenum, &boffset->offset);
    return true;
  }
  return false;
}

bool Partition::SetBinlogOffset(const BinlogOffset& boffset) {
  if (opened_) {
    logger_->SetProducerStatus(boffset.filenum, boffset.offset);
    return true;
  }
  return false;
}

void Partition::PrepareRsync() {
  slash::DeleteDirIfExist(dbsync_path_);
  slash::CreatePath(dbsync_path_ + "strings");
  slash::CreatePath(dbsync_path_ + "hashes");
  slash::CreatePath(dbsync_path_ + "lists");
  slash::CreatePath(dbsync_path_ + "sets");
  slash::CreatePath(dbsync_path_ + "zsets");
}

// Try to update master offset
// This may happend when dbsync from master finished
// Here we do:
// 1, Check dbsync finished, got the new binlog offset
// 2, Replace the old db
// 3, Update master offset, and the PikaAuxiliaryThread cron will connect and do slaveof task with master
bool Partition::TryUpdateMasterOffset() {
  std::string info_path = dbsync_path_ + kBgsaveInfoFile;
  if (!slash::FileExists(info_path)) {
    return false;
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
    g_pika_rm->GetSyncSlavePartitionByName(RmNode(table_name_, partition_id_));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << partition_name_ << " not exist";
    return false;
  }

  // Got new binlog offset
  std::ifstream is(info_path);
  if (!is) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to open info file after db sync";
    slave_partition->SetReplState(ReplState::kError);
    return false;
  }
  std::string line, master_ip;
  int lineno = 0;
  int64_t filenum = 0, offset = 0, tmp = 0, master_port = 0;
  while (std::getline(is, line)) {
    lineno++;
    if (lineno == 2) {
      master_ip = line;
    } else if (lineno > 2 && lineno < 6) {
      if (!slash::string2l(line.data(), line.size(), &tmp) || tmp < 0) {
        LOG(WARNING) << "Partition: " << partition_name_
            << ", Format of info file after db sync error, line : " << line;
        is.close();
        slave_partition->SetReplState(ReplState::kError);
        return false;
      }
      if (lineno == 3) { master_port = tmp; }
      else if (lineno == 4) { filenum = tmp; }
      else { offset = tmp; }

    } else if (lineno > 5) {
      LOG(WARNING) << "Partition: " << partition_name_
          << ", Format of info file after db sync error, line : " << line;
      is.close();
      slave_partition->SetReplState(ReplState::kError);
      return false;
    }
  }
  is.close();

  LOG(INFO) << "Partition: " << partition_name_ << " Information from dbsync info"
      << ",  master_ip: " << master_ip
      << ", master_port: " << master_port
      << ", filenum: " << filenum
      << ", offset: " << offset;

  // Sanity check
  if (master_ip != g_pika_server->master_ip() ||
      master_port != g_pika_server->master_port()) {
    LOG(WARNING) << "Partition: " << partition_name_
        << " Error master ip port: " << master_ip << ":" << master_port;
    slave_partition->SetReplState(ReplState::kError);
    return false;
  }

  slash::DeleteFile(info_path);
  if (!ChangeDb(dbsync_path_)) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to change db";
    slave_partition->SetReplState(ReplState::kError);
    return false;
  }

  // Update master offset
  logger_->SetProducerStatus(filenum, offset);
  slave_partition->SetReplState(ReplState::kTryConnect);
  return true;
}

/*
 * Change a new db locate in new_path
 * return true when change success
 * db remain the old one if return false
 */
bool Partition::ChangeDb(const std::string& new_path) {

  std::string tmp_path(db_path_);
  if (tmp_path.back() == '/') {
    tmp_path.resize(tmp_path.size() - 1);
  }
  tmp_path += "_bak";
  slash::DeleteDirIfExist(tmp_path);

  RWLock l(&db_rwlock_, true);
  LOG(INFO) << "Partition: "<< partition_name_
      << ", Prepare change db from: " << tmp_path;
  db_.reset();

  if (0 != slash::RenameFile(db_path_.c_str(), tmp_path)) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to rename db path when change db, error: " << strerror(errno);
    return false;
  }

  if (0 != slash::RenameFile(new_path.c_str(), db_path_.c_str())) {
    LOG(WARNING) << "Partition: " << partition_name_
        << ", Failed to rename new db path when change db, error: " << strerror(errno);
    return false;
  }

  db_.reset(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  slash::DeleteDirIfExist(tmp_path);
  LOG(INFO) << "Partition: " << partition_name_ << ", Change db success";
  return true;
}

bool Partition::IsBgSaving() {
  slash::MutexLock ml(&bgsave_protector_);
  return bgsave_info_.bgsaving;
}

void Partition::BgSavePartition() {
  slash::MutexLock l(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return;
  }
  bgsave_info_.bgsaving = true;
  BgTaskArg* bg_task_arg = new BgTaskArg();
  bg_task_arg->partition = shared_from_this();
  g_pika_server->BGSaveTaskSchedule(&DoBgSave, static_cast<void*>(bg_task_arg));
}

BgSaveInfo Partition::bgsave_info() {
  slash::MutexLock l(&bgsave_protector_);
  return bgsave_info_;
}

void Partition::DoBgSave(void* arg) {
  BgTaskArg* bg_task_arg = static_cast<BgTaskArg*>(arg);

  // Do BgSave
  bool success = bg_task_arg->partition->RunBgsaveEngine();

  // Some output
  BgSaveInfo info = bg_task_arg->partition->bgsave_info();
  std::ofstream out;
  out.open(info.path + "/" + kBgsaveInfoFile, std::ios::in | std::ios::trunc);
  if (out.is_open()) {
    out << (time(NULL) - info.start_time) << "s\n"
      << g_pika_server->host() << "\n"
      << g_pika_server->port() << "\n"
      << info.filenum << "\n"
      << info.offset << "\n";
    out.close();
  }
  if (!success) {
    std::string fail_path = info.path + "_FAILED";
    slash::RenameFile(info.path.c_str(), fail_path.c_str());
  }
  bg_task_arg->partition->FinishBgsave();

  delete bg_task_arg;
}

bool Partition::RunBgsaveEngine() {
  // Prepare for Bgsaving
  if (!InitBgsaveEnv() || !InitBgsaveEngine()) {
    ClearBgsave();
    return false;
  }
  LOG(INFO) << partition_name_ << " after prepare bgsave";

  BgSaveInfo info = bgsave_info();
  LOG(INFO) << partition_name_ << " bgsave_info: path=" << info.path
    << ",  filenum=" << info.filenum
    << ", offset=" << info.offset;

  // Backup to tmp dir
  rocksdb::Status s = bgsave_engine_->CreateNewBackup(info.path);
  LOG(INFO) << partition_name_ << " create new backup finished.";

  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " create new backup failed :" << s.ToString();
    return false;
  }
  return true;
}

// Prepare engine, need bgsave_protector protect
bool Partition::InitBgsaveEnv() {
  slash::MutexLock l(&bgsave_protector_);
  // Prepare for bgsave dir
  bgsave_info_.start_time = time(NULL);
  char s_time[32];
  int len = strftime(s_time, sizeof(s_time), "%Y%m%d%H%M%S", localtime(&bgsave_info_.start_time));
  bgsave_info_.s_start_time.assign(s_time, len);
  std::string time_sub_path = g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
  bgsave_info_.path = g_pika_conf->bgsave_path() + time_sub_path + "/" + bgsave_sub_path_;
  if (!slash::DeleteDirIfExist(bgsave_info_.path)) {
    LOG(WARNING) << partition_name_ << " remove exist bgsave dir failed";
    return false;
  }
  slash::CreatePath(bgsave_info_.path, 0755);
  // Prepare for failed dir
  if (!slash::DeleteDirIfExist(bgsave_info_.path + "_FAILED")) {
    LOG(WARNING) << partition_name_ << " remove exist fail bgsave dir failed :";
    return false;
  }
  return true;
}

// Prepare bgsave env, need bgsave_protector protect
bool Partition::InitBgsaveEngine() {
  delete bgsave_engine_;
  rocksdb::Status s = blackwidow::BackupEngine::Open(db().get(), &bgsave_engine_);
  if (!s.ok()) {
    LOG(WARNING) << partition_name_ << " open backup engine failed " << s.ToString();
    return false;
  }

  {
    RWLock l(&db_rwlock_, true);
    {
      slash::MutexLock l(&bgsave_protector_);
      logger_->GetProducerStatus(&bgsave_info_.filenum, &bgsave_info_.offset);
    }
    s = bgsave_engine_->SetBackupContent();
    if (!s.ok()) {
      LOG(WARNING) << partition_name_ << " set backup content failed " << s.ToString();
      return false;
    }
  }
  return true;
}

void Partition::ClearBgsave() {
  slash::MutexLock l(&bgsave_protector_);
  bgsave_info_.Clear();
}

void Partition::FinishBgsave() {
  slash::MutexLock l(&bgsave_protector_);
  bgsave_info_.bgsaving = false;
}

bool Partition::FlushDB() {
  slash::RWLock rwl(&db_rwlock_, true);
  slash::MutexLock ml(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << partition_name_ << " Delete old db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] == '/') {
    dbpath.erase(dbpath.length() - 1);
  }
  dbpath.append("_deleting/");
  slash::RenameFile(db_path_, dbpath.c_str());

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " Open new db success";
  g_pika_server->PurgeDir(dbpath);
  return true;
}

bool Partition::FlushSubDB(const std::string& db_name) {
  slash::RWLock rwl(&db_rwlock_, true);
  slash::MutexLock ml(&bgsave_protector_);
  if (bgsave_info_.bgsaving) {
    return false;
  }

  LOG(INFO) << partition_name_ << " Delete old " + db_name + " db...";
  db_.reset();

  std::string dbpath = db_path_;
  if (dbpath[dbpath.length() - 1] != '/') {
    dbpath.append("/");
  }

  std::string sub_dbpath = dbpath + db_name;
  std::string del_dbpath = dbpath + db_name + "_deleting";
  slash::RenameFile(sub_dbpath, del_dbpath);

  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(g_pika_server->bw_options(), db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " open new " + db_name + " db success";
  g_pika_server->PurgeDir(del_dbpath);
  return true;
}

bool Partition::PurgeLogs(uint32_t to, bool manual) {
  // Only one thread can go through
  bool expect = false;
  if (!purging_.compare_exchange_strong(expect, true)) {
    LOG(WARNING) << "purge process already exist";
    return false;
  }
  PurgeArg *arg = new PurgeArg();
  arg->to = to;
  arg->manual = manual;
  arg->partition = shared_from_this();
  g_pika_server->PurgelogsTaskSchedule(&DoPurgeLogs, static_cast<void*>(arg));
  return true;
}

void Partition::ClearPurge() {
  purging_ = false;
}

void Partition::DoPurgeLogs(void* arg) {
  PurgeArg* purge = static_cast<PurgeArg*>(arg);
  purge->partition->PurgeFiles(purge->to, purge->manual);
  purge->partition->ClearPurge();
  delete (PurgeArg*)arg;
}

bool Partition::PurgeFiles(uint32_t to, bool manual) {
  std::map<uint32_t, std::string> binlogs;
  if (!GetBinlogFiles(binlogs)) {
    LOG(WARNING) << partition_name_ << " Could not get binlog files!";
    return false;
  }

  int delete_num = 0;
  struct stat file_stat;
  int remain_expire_num = binlogs.size() - g_pika_conf->expire_logs_nums();
  std::map<uint32_t, std::string>::iterator it;
  for (it = binlogs.begin(); it != binlogs.end(); ++it) {
    if ((manual && it->first <= to)                                                            // Manual purgelogsto
      || (remain_expire_num > 0)                                                               // Expire num trigger
      || (binlogs.size() - delete_num > 10                                                     // At lease remain 10 files
          && stat(((log_path_ + it->second)).c_str(), &file_stat) == 0
          && file_stat.st_mtime < time(NULL) - g_pika_conf->expire_logs_days() * 24 * 3600)) { // Expire time trigger
      // We check this every time to avoid lock when we do file deletion
      if (!g_pika_server->PartitionCouldPurge(table_name_, partition_id_, it->first)) {
        LOG(WARNING) << partition_name_ << " Could not purge "<< (it->first) << ", since it is already be used";
        return false;
      }

      // Do delete
      slash::Status s = slash::DeleteFile(log_path_ + it->second);
      if (s.ok()) {
        ++delete_num;
        --remain_expire_num;
      } else {
        LOG(WARNING) << partition_name_ << " Purge log file : " << (it->second) <<  " failed! error:" << s.ToString();
      }
    } else {
      // Break when face the first one not satisfied
      // Since the binlogs is order by the file index
      break;
    }
  }
  if (delete_num) {
    LOG(INFO) << partition_name_ << " Success purge "<< delete_num;
  }
  return true;
}

bool Partition::GetBinlogFiles(std::map<uint32_t, std::string>& binlogs) {
  std::vector<std::string> children;
  int ret = slash::GetChildren(log_path_, children);
  if (ret != 0) {
    LOG(WARNING) << partition_name_ << " Get all files in log path failed! error:" << ret;
    return false;
  }

  int64_t index = 0;
  std::string sindex;
  std::vector<std::string>::iterator it;
  for (it = children.begin(); it != children.end(); ++it) {
    if ((*it).compare(0, kBinlogPrefixLen, kBinlogPrefix) != 0) {
      continue;
    }
    sindex = (*it).substr(kBinlogPrefixLen);
    if (slash::string2l(sindex.c_str(), sindex.size(), &index) == 1) {
      binlogs.insert(std::pair<uint32_t, std::string>(static_cast<uint32_t>(index), *it));
    }
  }
  return true;
}

