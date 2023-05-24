// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_conf.h"

#include <glog/logging.h>

#include <strings.h>
#include <algorithm>

#include "pstd/include/env.h"

#include "include/pika_define.h"

PikaConf::PikaConf(const std::string& path)
    : pstd::BaseConf(path), conf_path_(path), local_meta_(std::make_unique<PikaMeta>()) {}


Status PikaConf::InternalGetTargetTable(const std::string& table_name, uint32_t* const target) {
  int32_t table_index = -1;
  for (size_t idx = 0; idx < table_structs_.size(); ++idx) {
    if (table_structs_[idx].table_name == table_name) {
      table_index = idx;
      break;
    }
  }
  if (table_index == -1) {
    return Status::NotFound("table : " + table_name + " not found");
  }
  *target = table_index;
  return Status::OK();
}

Status PikaConf::TablePartitionsSanityCheck(const std::string& table_name, const std::set<uint32_t>& partition_ids,
                                            bool is_add) {
  std::shared_lock l(rwlock_);
  uint32_t table_index = 0;
  Status s = InternalGetTargetTable(table_name, &table_index);
  if (!s.ok()) {
    return s;
  }
  // Sanity Check
  for (const auto& id : partition_ids) {
    if (id >= table_structs_[table_index].partition_num) {
      return Status::Corruption("partition index out of range");
    } else if (is_add && table_structs_[table_index].partition_ids.count(id) != 0) {
      return Status::Corruption("partition : " + std::to_string(id) + " exist");
    } else if (!is_add && table_structs_[table_index].partition_ids.count(id) == 0) {
      return Status::Corruption("partition : " + std::to_string(id) + " not exist");
    }
  }
  return Status::OK();
}

Status PikaConf::AddTablePartitions(const std::string& table_name, const std::set<uint32_t>& partition_ids) {
  Status s = TablePartitionsSanityCheck(table_name, partition_ids, true);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(rwlock_);
  uint32_t index = 0;
  s = InternalGetTargetTable(table_name, &index);
  if (s.ok()) {
    for (const auto& id : partition_ids) {
      table_structs_[index].partition_ids.insert(id);
    }
    s = local_meta_->StableSave(table_structs_);
  }
  return s;
}

Status PikaConf::RemoveTablePartitions(const std::string& table_name, const std::set<uint32_t>& partition_ids) {
  Status s = TablePartitionsSanityCheck(table_name, partition_ids, false);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(rwlock_);
  uint32_t index = 0;
  s = InternalGetTargetTable(table_name, &index);
  if (s.ok()) {
    for (const auto& id : partition_ids) {
      table_structs_[index].partition_ids.erase(id);
    }
    s = local_meta_->StableSave(table_structs_);
  }
  return s;
}

Status PikaConf::AddTable(const std::string& table_name, const uint32_t slot_num) {
  Status s = AddTableSanityCheck(table_name);
  if (!s.ok()) {
    return s;
  }
  std::lock_guard l(rwlock_);
  table_structs_.push_back({table_name, slot_num, {}});
  s = local_meta_->StableSave(table_structs_);
  return s;
}

Status PikaConf::DelTable(const std::string& table_name) {
  Status s = DelTableSanityCheck(table_name);
  if (!s.ok()) {
    return s;
  }
  std::lock_guard l(rwlock_);
  for (auto iter = table_structs_.begin(); iter != table_structs_.end(); iter++) {
    if (iter->table_name == table_name) {
      table_structs_.erase(iter);
      break;
    }
  }
  return local_meta_->StableSave(table_structs_);
}

Status PikaConf::AddTableSanityCheck(const std::string& table_name) {
  std::shared_lock l(rwlock_);
  uint32_t table_index = 0;
  Status s = InternalGetTargetTable(table_name, &table_index);
  if (!s.IsNotFound()) {
    return Status::Corruption("table: " + table_name + " already exist");
  }
  return Status::OK();
}

Status PikaConf::DelTableSanityCheck(const std::string& table_name) {
  std::shared_lock l(rwlock_);
  uint32_t table_index = 0;
  return InternalGetTargetTable(table_name, &table_index);
}

int PikaConf::Load() {
  int ret = LoadConf();
  if (ret != 0) {
    return ret;
  }

  GetConfInt("timeout", &timeout_);
  if (timeout_ < 0) {
    timeout_ = 60;  // 60s
  }
  GetConfStr("server-id", &server_id_);
  if (server_id_.empty()) {
    server_id_ = "1";
  } else if (PIKA_SERVER_ID_MAX < std::stoull(server_id_)) {
    server_id_ = "PIKA_SERVER_ID_MAX";
  }
  GetConfStr("requirepass", &requirepass_);
  GetConfStr("masterauth", &masterauth_);
  GetConfStr("userpass", &userpass_);
  GetConfInt("maxclients", &maxclients_);
  if (maxclients_ <= 0) {
    maxclients_ = 20000;
  }
  GetConfInt("root-connection-num", &root_connection_num_);
  if (root_connection_num_ < 0) {
    root_connection_num_ = 2;
  }

  std::string swe;
  GetConfStr("slowlog-write-errorlog", &swe);
  slowlog_write_errorlog_.store(swe == "yes" ? true : false);

  int tmp_slowlog_log_slower_than;
  GetConfInt("slowlog-log-slower-than", &tmp_slowlog_log_slower_than);
  slowlog_log_slower_than_.store(tmp_slowlog_log_slower_than);

  GetConfInt("slowlog-max-len", &slowlog_max_len_);
  if (slowlog_max_len_ == 0) {
    slowlog_max_len_ = 128;
  }
  std::string user_blacklist;
  GetConfStr("userblacklist", &user_blacklist);
  pstd::StringSplit(user_blacklist, COMMA, user_blacklist_);
  for (auto& item : user_blacklist_) {
    pstd::StringToLower(item);
  }

  GetConfStr("dump-path", &bgsave_path_);
  bgsave_path_ = bgsave_path_.empty() ? "./dump/" : bgsave_path_;
  if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
    bgsave_path_ += "/";
  }
  GetConfInt("dump-expire", &expire_dump_days_);
  if (expire_dump_days_ < 0) {
    expire_dump_days_ = 0;
  }
  GetConfStr("dump-prefix", &bgsave_prefix_);

  GetConfInt("expire-logs-nums", &expire_logs_nums_);
  if (expire_logs_nums_ <= 10) {
    expire_logs_nums_ = 10;
  }
  GetConfInt("expire-logs-days", &expire_logs_days_);
  if (expire_logs_days_ <= 0) {
    expire_logs_days_ = 1;
  }
  GetConfStr("compression", &compression_);
  GetConfStr("compression_per_level", &compression_per_level_);
  // set slave read only true as default
  slave_read_only_ = true;
  GetConfInt("slave-priority", &slave_priority_);

  //
  // Immutable Sections
  //
  GetConfInt("port", &port_);
  GetConfStr("log-path", &log_path_);
  log_path_ = log_path_.empty() ? "./log/" : log_path_;
  if (log_path_[log_path_.length() - 1] != '/') {
    log_path_ += "/";
  }
  GetConfStr("db-path", &db_path_);
  db_path_ = db_path_.empty() ? "./db/" : db_path_;
  if (db_path_[db_path_.length() - 1] != '/') {
    db_path_ += "/";
  }
  local_meta_->SetPath(db_path_);

  GetConfInt("thread-num", &thread_num_);
  if (thread_num_ <= 0) {
    thread_num_ = 12;
  }

  GetConfInt("thread-pool-size", &thread_pool_size_);
  if (thread_pool_size_ <= 0) {
    thread_pool_size_ = 12;
  }
  if (thread_pool_size_ > 100) {
    thread_pool_size_ = 100;
  }
  GetConfInt("sync-thread-num", &sync_thread_num_);
  if (sync_thread_num_ <= 0) {
    sync_thread_num_ = 3;
  }
  if (sync_thread_num_ > 24) {
    sync_thread_num_ = 24;
  }

  std::string instance_mode;
  GetConfStr("instance-mode", &instance_mode);
  classic_mode_.store(instance_mode.empty() || !strcasecmp(instance_mode.data(), "classic"));

  if (classic_mode_.load()) {
    GetConfInt("databases", &databases_);
    if (databases_ < 1 || databases_ > 8) {
      LOG(FATAL) << "config databases error, limit [1 ~ 8], the actual is: " << databases_;
    }
    for (int idx = 0; idx < databases_; ++idx) {
      table_structs_.push_back({"db" + std::to_string(idx), 1, {0}});
    }
  }
  default_table_ = table_structs_[0].table_name;

  int tmp_replication_num = 0;
  GetConfInt("replication-num", &tmp_replication_num);
  if (tmp_replication_num > 4 || tmp_replication_num < 0) {
    LOG(FATAL) << "replication-num " << tmp_replication_num << "is invalid, please pick from [0...4]";
  }
  replication_num_.store(tmp_replication_num);

  int tmp_consensus_level = 0;
  GetConfInt("consensus-level", &tmp_consensus_level);
  if (tmp_consensus_level < 0 || tmp_consensus_level > replication_num_.load()) {
    LOG(FATAL) << "consensus-level " << tmp_consensus_level
               << " is invalid, current replication-num: " << replication_num_.load()
               << ", please pick from 0 to replication-num"
               << " [0..." << replication_num_.load() << "]";
  }
  consensus_level_.store(tmp_consensus_level);
  if (classic_mode_.load() && (consensus_level_.load() != 0 || replication_num_.load() != 0)) {
    LOG(FATAL) << "consensus-level & replication-num only configurable under sharding mode,"
               << " set it to be 0 if you are using classic mode";
  }

  compact_cron_ = "";
  GetConfStr("compact-cron", &compact_cron_);
  if (compact_cron_ != "") {
    bool have_week = false;
    std::string compact_cron, week_str;
    int slash_num = count(compact_cron_.begin(), compact_cron_.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = compact_cron_.find("/");
      week_str = compact_cron_.substr(0, first_slash);
      compact_cron = compact_cron_.substr(first_slash + 1);
    } else {
      compact_cron = compact_cron_;
    }

    std::string::size_type len = compact_cron.length();
    std::string::size_type colon = compact_cron.find("-");
    std::string::size_type underline = compact_cron.find("/");
    if (colon == std::string::npos || underline == std::string::npos || colon >= underline || colon + 1 >= len ||
        colon + 1 == underline || underline + 1 >= len) {
      compact_cron_ = "";
    } else {
      int week = std::atoi(week_str.c_str());
      int start = std::atoi(compact_cron.substr(0, colon).c_str());
      int end = std::atoi(compact_cron.substr(colon + 1, underline).c_str());
      int usage = std::atoi(compact_cron.substr(underline + 1).c_str());
      if ((have_week && (week < 1 || week > 7)) || start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 ||
          usage > 100) {
        compact_cron_ = "";
      }
    }
  }

  compact_interval_ = "";
  GetConfStr("compact-interval", &compact_interval_);
  if (compact_interval_ != "") {
    std::string::size_type len = compact_interval_.length();
    std::string::size_type slash = compact_interval_.find("/");
    if (slash == std::string::npos || slash + 1 >= len) {
      compact_interval_ = "";
    } else {
      int interval = std::atoi(compact_interval_.substr(0, slash).c_str());
      int usage = std::atoi(compact_interval_.substr(slash + 1).c_str());
      if (interval <= 0 || usage < 0 || usage > 100) {
        compact_interval_ = "";
      }
    }
  }

  // write_buffer_size
  GetConfInt64Human("write-buffer-size", &write_buffer_size_);
  if (write_buffer_size_ <= 0) {
    write_buffer_size_ = 268435456;  // 256Mb
  }

  // arena_block_size
  GetConfInt64Human("arena-block-size", &arena_block_size_);
  if (arena_block_size_ <= 0) {
    arena_block_size_ = write_buffer_size_ >> 3;  // 1/8 of the write_buffer_size_
  }

  // max_write_buffer_size
  GetConfInt64Human("max-write-buffer-size", &max_write_buffer_size_);
  if (max_write_buffer_size_ <= 0) {
    max_write_buffer_size_ = 10737418240;  // 10Gb
  }

  // rate-limiter-bandwidth
  GetConfInt64("rate-limiter-bandwidth", &rate_limiter_bandwidth_);
  if (rate_limiter_bandwidth_ <= 0) {
    rate_limiter_bandwidth_ = 200 * 1024 * 1024;  // 200MB
  }

  // rate-limiter-refill-period-us
  GetConfInt64("rate-limiter-refill-period-us", &rate_limiter_refill_period_us_);
  if (rate_limiter_refill_period_us_ <= 0) {
    rate_limiter_refill_period_us_ = 100 * 1000;
  }

  // rate-limiter-fairness
  GetConfInt64("rate-limiter-fairness", &rate_limiter_fairness_);
  if (rate_limiter_fairness_ <= 0) {
    rate_limiter_fairness_ = 10;
  }

  std::string at;
  GetConfStr("rate-limiter-auto-tuned", &at);
  rate_limiter_auto_tuned_ = (at == "yes" || at.empty()) ? true : false;

  // max_write_buffer_num
  max_write_buffer_num_ = 2;
  GetConfInt("max-write-buffer-num", &max_write_buffer_num_);
  if (max_write_buffer_num_ <= 0) {
    max_write_buffer_num_ = 2;  // 1 for immutable memtable, 1 for mutable memtable
  }

  // max_client_response_size
  GetConfInt64Human("max-client-response-size", &max_client_response_size_);
  if (max_client_response_size_ <= 0) {
    max_client_response_size_ = 1073741824;  // 1Gb
  }

  // target_file_size_base
  GetConfIntHuman("target-file-size-base", &target_file_size_base_);
  if (target_file_size_base_ <= 0) {
    target_file_size_base_ = 1048576;  // 10Mb
  }

  max_cache_statistic_keys_ = 0;
  GetConfInt("max-cache-statistic-keys", &max_cache_statistic_keys_);
  if (max_cache_statistic_keys_ <= 0) {
    max_cache_statistic_keys_ = 0;
  }

  small_compaction_threshold_ = 5000;
  GetConfInt("small-compaction-threshold", &small_compaction_threshold_);
  if (small_compaction_threshold_ <= 0 || small_compaction_threshold_ >= 100000) {
    small_compaction_threshold_ = 5000;
  }

  max_background_flushes_ = 1;
  GetConfInt("max-background-flushes", &max_background_flushes_);
  if (max_background_flushes_ <= 0) {
    max_background_flushes_ = 1;
  }
  if (max_background_flushes_ >= 4) {
    max_background_flushes_ = 4;
  }

  max_background_compactions_ = 2;
  GetConfInt("max-background-compactions", &max_background_compactions_);
  if (max_background_compactions_ <= 0) {
    max_background_compactions_ = 2;
  }
  if (max_background_compactions_ >= 8) {
    max_background_compactions_ = 8;
  }

  max_cache_files_ = 5000;
  GetConfInt("max-cache-files", &max_cache_files_);
  if (max_cache_files_ < -1) {
    max_cache_files_ = 5000;
  }
  max_bytes_for_level_multiplier_ = 10;
  GetConfInt("max-bytes-for-level-multiplier", &max_bytes_for_level_multiplier_);
  if (max_bytes_for_level_multiplier_ < 10) {
    max_bytes_for_level_multiplier_ = 5;
  }

  block_size_ = 4 * 1024;
  GetConfInt64Human("block-size", &block_size_);
  if (block_size_ <= 0) {
    block_size_ = 4 * 1024;
  }

  block_cache_ = 8 * 1024 * 1024;
  GetConfInt64Human("block-cache", &block_cache_);
  if (block_cache_ < 0) {
    block_cache_ = 8 * 1024 * 1024;
  }

  num_shard_bits_ = -1;
  GetConfInt64("num-shard-bits", &num_shard_bits_);

  std::string sbc;
  GetConfStr("share-block-cache", &sbc);
  share_block_cache_ = (sbc == "yes") ? true : false;

  std::string ciafb;
  GetConfStr("cache-index-and-filter-blocks", &ciafb);
  cache_index_and_filter_blocks_ = (ciafb == "yes") ? true : false;

  std::string plfaibic;
  GetConfStr("pin_l0_filter_and_index_blocks_in_cache", &plfaibic);
  pin_l0_filter_and_index_blocks_in_cache_ = (plfaibic == "yes") ? true : false;

  std::string offh;
  GetConfStr("optimize-filters-for-hits", &offh);
  optimize_filters_for_hits_ = (offh == "yes") ? true : false;

  std::string lcdlb;
  GetConfStr("level-compaction-dynamic-level-bytes", &lcdlb);
  level_compaction_dynamic_level_bytes_ = (lcdlb == "yes") ? true : false;

  // daemonize
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ = (dmz == "yes") ? true : false;

  // binlog
  std::string wb;
  GetConfStr("write-binlog", &wb);
  write_binlog_ = (wb == "no") ? false : true;
  GetConfIntHuman("binlog-file-size", &binlog_file_size_);
  if (binlog_file_size_ < 1024 || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024)) {
    binlog_file_size_ = 100 * 1024 * 1024;  // 100M
  }
  GetConfStr("pidfile", &pidfile_);

  // db sync
  GetConfStr("db-sync-path", &db_sync_path_);
  db_sync_path_ = db_sync_path_.empty() ? "./dbsync/" : db_sync_path_;
  if (db_sync_path_[db_sync_path_.length() - 1] != '/') {
    db_sync_path_ += "/";
  }
  GetConfInt("db-sync-speed", &db_sync_speed_);
  if (db_sync_speed_ < 0 || db_sync_speed_ > 1024) {
    db_sync_speed_ = 1024;
  }
  // network interface
  network_interface_ = "";
  GetConfStr("network-interface", &network_interface_);

  // slaveof
  slaveof_ = "";
  GetConfStr("slaveof", &slaveof_);

  // sync window size
  int tmp_sync_window_size = kBinlogReadWinDefaultSize;
  GetConfInt("sync-window-size", &tmp_sync_window_size);
  if (tmp_sync_window_size <= 0) {
    sync_window_size_.store(kBinlogReadWinDefaultSize);
  } else if (tmp_sync_window_size > kBinlogReadWinMaxSize) {
    sync_window_size_.store(kBinlogReadWinMaxSize);
  } else {
    sync_window_size_.store(tmp_sync_window_size);
  }

  // max conn rbuf size
  int tmp_max_conn_rbuf_size = PIKA_MAX_CONN_RBUF;
  GetConfIntHuman("max-conn-rbuf-size", &tmp_max_conn_rbuf_size);
  if (tmp_max_conn_rbuf_size == PIKA_MAX_CONN_RBUF_LB || tmp_max_conn_rbuf_size == PIKA_MAX_CONN_RBUF_HB) {
    max_conn_rbuf_size_.store(tmp_max_conn_rbuf_size);
  } else {
    max_conn_rbuf_size_.store(PIKA_MAX_CONN_RBUF);
  }

  // rocksdb blob configure
  GetConfBool("enable-blob-files", &enable_blob_files_);
  GetConfInt64("min-blob-size", &min_blob_size_);
  if (min_blob_size_ <= 0) {
    min_blob_size_ = 4096;
  }
  GetConfInt64Human("blob-file-size", &blob_file_size_);
  if (blob_file_size_ <= 0) {
    blob_file_size_ = 256 * 1024 * 1024;
  }
  GetConfStr("blob-compression-type", &blob_compression_type_);
  GetConfBool("enable-blob-garbage-collection", &enable_blob_garbage_collection_);
  GetConfDouble("blob-garbage-collection-age-cutoff", &blob_garbage_collection_age_cutoff_);
  if (blob_garbage_collection_age_cutoff_ <= 0) {
    blob_garbage_collection_age_cutoff_ = 0.25;
  }
  GetConfDouble("blob-garbage-collection-force-threshold", &blob_garbage_collection_force_threshold_);
  if (blob_garbage_collection_force_threshold_ <= 0) {
    blob_garbage_collection_force_threshold_ = 1.0;
  }
  GetConfInt64("blob-cache", &block_cache_);
  GetConfInt64("blob-num-shard-bits", &blob_num_shard_bits_);

  return ret;
}

void PikaConf::TryPushDiffCommands(const std::string& command, const std::string& value) {
  if (!CheckConfExist(command)) {
    diff_commands_[command] = value;
  }
}

int PikaConf::ConfigRewrite() {
  std::string userblacklist = suser_blacklist();

  std::lock_guard l(rwlock_);
  // Only set value for config item that can be config set.
  SetConfInt("timeout", timeout_);
  SetConfStr("requirepass", requirepass_);
  SetConfStr("masterauth", masterauth_);
  SetConfStr("userpass", userpass_);
  SetConfStr("userblacklist", userblacklist);
  SetConfStr("dump-prefix", bgsave_prefix_);
  SetConfInt("maxclients", maxclients_);
  SetConfInt("dump-expire", expire_dump_days_);
  SetConfInt("expire-logs-days", expire_logs_days_);
  SetConfInt("expire-logs-nums", expire_logs_nums_);
  SetConfInt("root-connection-num", root_connection_num_);
  SetConfStr("slowlog-write-errorlog", slowlog_write_errorlog_.load() ? "yes" : "no");
  SetConfInt("slowlog-log-slower-than", slowlog_log_slower_than_.load());
  SetConfInt("slowlog-max-len", slowlog_max_len_);
  SetConfStr("write-binlog", write_binlog_ ? "yes" : "no");
  SetConfInt("max-cache-statistic-keys", max_cache_statistic_keys_);
  SetConfInt("small-compaction-threshold", small_compaction_threshold_);
  SetConfInt("max-client-response-size", max_client_response_size_);
  SetConfInt("db-sync-speed", db_sync_speed_);
  SetConfStr("compact-cron", compact_cron_);
  SetConfStr("compact-interval", compact_interval_);
  SetConfInt("slave-priority", slave_priority_);
  SetConfInt("sync-window-size", sync_window_size_.load());
  SetConfInt("consensus-level", consensus_level_.load());
  SetConfInt("replication-num", replication_num_.load());
  // options for storage engine
  SetConfInt("max-cache-files", max_cache_files_);
  SetConfInt("max-background-compactions", max_background_compactions_);
  SetConfInt("max-write-buffer-num", max_write_buffer_num_);
  SetConfInt64("write-buffer-size", write_buffer_size_);
  SetConfInt64("arena-block-size", arena_block_size_);
  // slaveof config item is special
  SetConfStr("slaveof", slaveof_);

  if (!diff_commands_.empty()) {
    std::vector<pstd::BaseConf::Rep::ConfItem> filtered_items;
    for (const auto& diff_command : diff_commands_) {
      if (!diff_command.second.empty()) {
        pstd::BaseConf::Rep::ConfItem item(pstd::BaseConf::Rep::kConf, diff_command.first, diff_command.second);
        filtered_items.push_back(item);
      }
    }
    if (!filtered_items.empty()) {
      pstd::BaseConf::Rep::ConfItem comment_item(pstd::BaseConf::Rep::kComment, "# Generated by CONFIG REWRITE\n");
      PushConfItem(comment_item);
      for (const auto& item : filtered_items) {
        PushConfItem(item);
      }
    }
    diff_commands_.clear();
  }
  return WriteBack();
}

rocksdb::CompressionType PikaConf::GetCompression(const std::string& value) {
  if (value == "snappy") {
    return rocksdb::CompressionType::kSnappyCompression;
  } else if (value == "zlib") {
    return rocksdb::CompressionType::kZlibCompression;
  } else if (value == "lz4") {
    return rocksdb::CompressionType::kLZ4Compression;
  } else if (value == "zstd") {
    return rocksdb::CompressionType::kZSTD;
  }
  return rocksdb::CompressionType::kNoCompression;
}

std::vector<rocksdb::CompressionType> PikaConf::compression_per_level() {
  std::shared_lock l(rwlock_);
  std::vector<rocksdb::CompressionType> types;
  if (compression_per_level_.empty()) {
    return types;
  }
  auto left = compression_per_level_.find_first_of('[');
  auto right = compression_per_level_.find_first_of(']');

  if (left == std::string::npos || right == std::string::npos || right <= left + 1) {
    return types;
  }
  std::vector<std::string> strings;
  pstd::StringSplit(compression_per_level_.substr(left + 1, right - left - 1), ':', strings);
  for (const auto& item : strings) {
    types.push_back(GetCompression(pstd::StringTrim(item)));
  }
  return types;
}
