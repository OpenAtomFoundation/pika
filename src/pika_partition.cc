// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_partition.h"

extern PikaConf *g_pika_conf;
extern PikaServer* g_pika_server;

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

Partition::Partition(const std::string& table_name,
                     uint32_t partition_id,
                     const std::string& table_db_path,
                     const std::string& table_log_path) :
  table_name_(table_name),
  partition_id_(partition_id),
  binlog_io_error_(false),
  bgsave_engine_(NULL) {

  db_path_ = PartitionPath(table_db_path, partition_id_);
  log_path_ = PartitionPath(table_log_path, partition_id_);
  partition_name_ = PartitionName(table_name_, partition_id_);

  pthread_rwlock_init(&db_rwlock_, NULL);

  //Create blackwidow handle
  blackwidow::BlackwidowOptions bw_option;
  RocksdbOptionInit(&bw_option);

  LOG(INFO) << partition_name_ << " prepare Blackwidow DB...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(bw_option, db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " DB Success";

  logger_ = std::shared_ptr<Binlog>(
          new Binlog(log_path_, g_pika_conf->binlog_file_size()));
}

Partition::~Partition() {
  pthread_rwlock_destroy(&db_rwlock_);
  db_.reset();
  logger_.reset();
  delete bgsave_engine_;
}

uint32_t Partition::partition_id() const {
  return partition_id_;
}

std::string Partition::partition_name() const {
  return partition_name_;
}

std::shared_ptr<Binlog> Partition::logger() const {
  return logger_;
}

std::shared_ptr<blackwidow::BlackWidow> Partition::db() const {
  return db_;
}

void Partition::DoCommand(Cmd* const cmd) {
  slash::RWLock rw(&db_rwlock_, false);
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
      LOG(WARNING) << partition_name() << " Writing binlog failed, maybe no space left on device";
      SetBinlogIoError(true);
      cmd->res().SetRes(CmdRes::kErrOther, "Writing binlog failed, maybe no space left on device");
    }
  }

  if (cmd->is_write()) {
    RecordUnLock(cmd->current_key());
  }
}

void Partition::BinlogLock() {
  logger_->Lock();
}

void Partition::BinlogUnLock() {
  logger_->Unlock();
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

BGSaveInfo Partition::bgsave_info() {
  slash::MutexLock l(&bgsave_protector_);
  return bgsave_info_;
}

void Partition::DoBgSave(void* arg) {
  BgTaskArg* bg_task_arg = static_cast<BgTaskArg*>(arg);

  // Do BgSave
  bool success = bg_task_arg->partition->RunBgsaveEngine();

  // Some output
  BGSaveInfo info = bg_task_arg->partition->bgsave_info();
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

  BGSaveInfo info = bgsave_info();
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
  std::string bgsave_path(g_pika_conf->bgsave_path() + table_name_ + "/" + std::to_string(partition_id_) + "/");
  bgsave_info_.path = bgsave_path + g_pika_conf->bgsave_prefix() + std::string(s_time, 8);
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
    if (!s.ok()){
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

bool Partition::FlushAll() {
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

  //Create blackwidow handle
  blackwidow::BlackwidowOptions bw_option;
  RocksdbOptionInit(&bw_option);

  LOG(INFO) << partition_name_ << " Prepare open new db...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(bw_option, db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " open new db success";
  PurgeDir(dbpath);
  return true;
}

bool Partition::FlushDb(const std::string& db_name) {
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

  // create blackwidow handle
  blackwidow::BlackwidowOptions bw_option;
  RocksdbOptionInit(&bw_option);

  LOG(INFO) << partition_name_ << " Prepare open new " + db_name + " db...";
  db_ = std::shared_ptr<blackwidow::BlackWidow>(new blackwidow::BlackWidow());
  rocksdb::Status s = db_->Open(bw_option, db_path_);
  assert(db_);
  assert(s.ok());
  LOG(INFO) << partition_name_ << " open new " + db_name + " db success";
  PurgeDir(del_dbpath);
  return true;
}

void Partition::PurgeDir(std::string& path) {
  std::string *dir_path = new std::string(path);
  g_pika_server->PurgeDirTaskSchedule(&DoPurgeDir, static_cast<void*>(dir_path));
}

void Partition::DoPurgeDir(void* arg) {
  std::string path = *(static_cast<std::string*>(arg));
  LOG(INFO) << "Delete dir: " << path << " start";
  slash::DeleteDir(path);
  LOG(INFO) << "Delete dir: " << path << " done";
  delete static_cast<std::string*>(arg);
}

void Partition::RocksdbOptionInit(blackwidow::BlackwidowOptions* bw_option) const {
  bw_option->options.create_if_missing = true;
  bw_option->options.keep_log_file_num = 10;
  bw_option->options.max_manifest_file_size = 64 * 1024 * 1024;
  bw_option->options.max_log_file_size = 512 * 1024 * 1024;

  bw_option->options.write_buffer_size =
                g_pika_conf->write_buffer_size();
  bw_option->options.target_file_size_base =
                g_pika_conf->target_file_size_base();
  bw_option->options.max_background_flushes =
                g_pika_conf->max_background_flushes();
  bw_option->options.max_background_compactions =
                g_pika_conf->max_background_compactions();
  bw_option->options.max_open_files =
                g_pika_conf->max_cache_files();
  bw_option->options.max_bytes_for_level_multiplier =
                g_pika_conf->max_bytes_for_level_multiplier();
  bw_option->options.optimize_filters_for_hits =
                g_pika_conf->optimize_filters_for_hits();
  bw_option->options.level_compaction_dynamic_level_bytes =
                g_pika_conf->level_compaction_dynamic_level_bytes();

  if (g_pika_conf->compression() == "none") {
    bw_option->options.compression =
        rocksdb::CompressionType::kNoCompression;
  } else if (g_pika_conf->compression() == "snappy") {
    bw_option->options.compression =
        rocksdb::CompressionType::kSnappyCompression;
  } else if (g_pika_conf->compression() == "zlib") {
    bw_option->options.compression =
        rocksdb::CompressionType::kZlibCompression;
  }

  bw_option->table_options.block_size = g_pika_conf->block_size();
  bw_option->table_options.cache_index_and_filter_blocks =
      g_pika_conf->cache_index_and_filter_blocks();
  bw_option->block_cache_size = g_pika_conf->block_cache();
  bw_option->share_block_cache = g_pika_conf->share_block_cache();
  bw_option->statistics_max_size = g_pika_conf->max_cache_statistic_keys();
  bw_option->small_compaction_threshold =
      g_pika_conf->small_compaction_threshold();
}
