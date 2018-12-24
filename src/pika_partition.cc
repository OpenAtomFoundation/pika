// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_partition.h"

extern PikaConf *g_pika_conf;

std::string PartitionPath(const std::string& table_path,
                          uint32_t partition_id) {
  char buf[100];
  snprintf(buf, sizeof(buf), "%u/", partition_id);
  return table_path + buf;
}

std::string PartitionName(const std::string& table_name,
                          uint32_t partition_id) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%s_%u", table_name.data(), partition_id);
  return std::string(buf);
}

Partition::Partition(const std::string& table_name,
                     uint32_t partition_id,
                     const std::string& table_db_path,
                     const std::string& table_log_path) :
  table_name_(table_name),
  partition_id_(partition_id),
  binlog_io_error_(false) {

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
