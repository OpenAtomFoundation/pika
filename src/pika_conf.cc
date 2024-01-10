// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_conf.h"

#include <glog/logging.h>

#include <strings.h>
#include <algorithm>

#include "pstd/include/env.h"
#include "pstd/include/pstd_string.h"

#include "cache/include/config.h"
#include "include/acl.h"
#include "include/pika_define.h"

using pstd::Status;

PikaConf::PikaConf(const std::string& path)
    : pstd::BaseConf(path), conf_path_(path), local_meta_(std::make_unique<PikaMeta>()) {}

Status PikaConf::InternalGetTargetDB(const std::string& db_name, uint32_t* const target) {
  int32_t db_index = -1;
  for (size_t idx = 0; idx < db_structs_.size(); ++idx) {
    if (db_structs_[idx].db_name == db_name) {
      db_index = static_cast<int32_t>(idx);
      break;
    }
  }
  if (db_index == -1) {
    return Status::NotFound("db : " + db_name + " not found");
  }
  *target = db_index;
  return Status::OK();
}

Status PikaConf::DBSlotsSanityCheck(const std::string& db_name, const std::set<uint32_t>& slot_ids, bool is_add) {
  std::shared_lock l(rwlock_);
  uint32_t db_index = 0;
  Status s = InternalGetTargetDB(db_name, &db_index);
  if (!s.ok()) {
    return s;
  }
  // Sanity Check
  for (const auto& id : slot_ids) {
    if (id >= db_structs_[db_index].slot_num) {
      return Status::Corruption("slot index out of range");
    } else if (is_add && db_structs_[db_index].slot_ids.count(id) != 0) {
      return Status::Corruption("slot : " + std::to_string(id) + " exist");
    } else if (!is_add && db_structs_[db_index].slot_ids.count(id) == 0) {
      return Status::Corruption("slot : " + std::to_string(id) + " not exist");
    }
  }
  return Status::OK();
}

Status PikaConf::AddDBSlots(const std::string& db_name, const std::set<uint32_t>& slot_ids) {
  Status s = DBSlotsSanityCheck(db_name, slot_ids, true);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(rwlock_);
  uint32_t index = 0;
  s = InternalGetTargetDB(db_name, &index);
  if (s.ok()) {
    for (const auto& id : slot_ids) {
      db_structs_[index].slot_ids.insert(id);
    }
    s = local_meta_->StableSave(db_structs_);
  }
  return s;
}

Status PikaConf::RemoveDBSlots(const std::string& db_name, const std::set<uint32_t>& slot_ids) {
  Status s = DBSlotsSanityCheck(db_name, slot_ids, false);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(rwlock_);
  uint32_t index = 0;
  s = InternalGetTargetDB(db_name, &index);
  if (s.ok()) {
    for (const auto& id : slot_ids) {
      db_structs_[index].slot_ids.erase(id);
    }
    s = local_meta_->StableSave(db_structs_);
  }
  return s;
}

Status PikaConf::AddDB(const std::string& db_name, const uint32_t slot_num) {
  Status s = AddDBSanityCheck(db_name);
  if (!s.ok()) {
    return s;
  }
  std::lock_guard l(rwlock_);
  db_structs_.push_back({db_name, slot_num, {}});
  s = local_meta_->StableSave(db_structs_);
  return s;
}

Status PikaConf::DelDB(const std::string& db_name) {
  Status s = DelDBSanityCheck(db_name);
  if (!s.ok()) {
    return s;
  }
  std::lock_guard l(rwlock_);
  for (auto iter = db_structs_.begin(); iter != db_structs_.end(); iter++) {
    if (iter->db_name == db_name) {
      db_structs_.erase(iter);
      break;
    }
  }
  return local_meta_->StableSave(db_structs_);
}

Status PikaConf::AddDBSanityCheck(const std::string& db_name) {
  std::shared_lock l(rwlock_);
  uint32_t db_index = 0;
  Status s = InternalGetTargetDB(db_name, &db_index);
  if (!s.IsNotFound()) {
    return Status::Corruption("db: " + db_name + " already exist");
  }
  return Status::OK();
}

Status PikaConf::DelDBSanityCheck(const std::string& db_name) {
  std::shared_lock l(rwlock_);
  uint32_t db_index = 0;
  return InternalGetTargetDB(db_name, &db_index);
}

int PikaConf::Load() {
  int ret = LoadConf();
  if (ret) {
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
  GetConfStr("run-id", &run_id_);
  if (run_id_.empty()) {
    run_id_ = pstd::getRandomHexChars(configRunIDSize);
    // try rewrite run_id_ to diff_commands_
    SetRunID(run_id_);
  } else if (run_id_.length() != configRunIDSize) {
    LOG(FATAL) << "run-id " << run_id_ << " is invalid, its string length should be " << configRunIDSize;
  }
  GetConfStr("replication-id", &replication_id_);
  GetConfStr("requirepass", &requirepass_);
  GetConfStr("masterauth", &masterauth_);
  //  GetConfStr("userpass", &userpass_);
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

  // slot migrate
  std::string smgrt = "no";
  GetConfStr("slotmigrate", &smgrt);
  slotmigrate_ = (smgrt == "yes") ? true : false;

  int binlog_writer_num = 1;
  GetConfInt("binlog-writer-num", &binlog_writer_num);
  if (binlog_writer_num <= 0 || binlog_writer_num > 24) {
    binlog_writer_num_ = 1;
  } else {
    binlog_writer_num_ = binlog_writer_num;
  }

  int tmp_slowlog_log_slower_than;
  GetConfInt("slowlog-log-slower-than", &tmp_slowlog_log_slower_than);
  slowlog_log_slower_than_.store(tmp_slowlog_log_slower_than);

  GetConfInt("slowlog-max-len", &slowlog_max_len_);
  if (slowlog_max_len_ == 0) {
    slowlog_max_len_ = 128;
  }

  GetConfInt("default-slot-num", &default_slot_num_);
  if (default_slot_num_ <= 0) {
    LOG(FATAL) << "config default-slot-num error,"
               << " it should greater than zero, the actual is: " << default_slot_num_;
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
  GetConfStr("loglevel", &log_level_);
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
      db_structs_.push_back({"db" + std::to_string(idx), 1, {0}});
    }
  }
  default_db_ = db_structs_[0].db_name;

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
  if (!compact_cron_.empty()) {
    bool have_week = false;
    std::string compact_cron;
    std::string week_str;
    int64_t slash_num = count(compact_cron_.begin(), compact_cron_.end(), '/');
    if (slash_num == 2) {
      have_week = true;
      std::string::size_type first_slash = compact_cron_.find('/');
      week_str = compact_cron_.substr(0, first_slash);
      compact_cron = compact_cron_.substr(first_slash + 1);
    } else {
      compact_cron = compact_cron_;
    }

    std::string::size_type len = compact_cron.length();
    std::string::size_type colon = compact_cron.find('-');
    std::string::size_type underline = compact_cron.find('/');
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
  if (!compact_interval_.empty()) {
    std::string::size_type len = compact_interval_.length();
    std::string::size_type slash = compact_interval_.find('/');
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

  // least-free-disk-resume-size
  GetConfInt64Human("least-free-disk-resume-size", &least_free_disk_to_resume_);
  if (least_free_disk_to_resume_ <= 0) {
    least_free_disk_to_resume_ = 268435456;  // 256Mb
  }

  GetConfInt64("manually-resume-interval", &resume_check_interval_);
  if (resume_check_interval_ <= 0) {
    resume_check_interval_ = 60;  // seconds
  }

  GetConfDouble("min-check-resume-ratio", &min_check_resume_ratio_);
  if (min_check_resume_ratio_ < 0) {
    min_check_resume_ratio_ = 0.7;
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

  // arena_block_size
  GetConfInt64Human("slotmigrate-thread-num_", &slotmigrate_thread_num_);
  if (slotmigrate_thread_num_ < 1 || slotmigrate_thread_num_ > 24) {
    slotmigrate_thread_num_ = 8;  // 1/8 of the write_buffer_size_
  }

  // arena_block_size
  GetConfInt64Human("thread-migrate-keys-num", &thread_migrate_keys_num_);
  if (thread_migrate_keys_num_ < 64 || thread_migrate_keys_num_ > 128) {
    thread_migrate_keys_num_ = 64;  // 1/8 of the write_buffer_size_
  }

  // max_write_buffer_size
  GetConfInt64Human("max-write-buffer-size", &max_write_buffer_size_);
  if (max_write_buffer_size_ <= 0) {
    max_write_buffer_size_ = PIKA_CACHE_SIZE_DEFAULT;  // 10Gb
  }

  // rate-limiter-bandwidth
  GetConfInt64("rate-limiter-bandwidth", &rate_limiter_bandwidth_);
  if (rate_limiter_bandwidth_ <= 0) {
    rate_limiter_bandwidth_ = 2000 * 1024 * 1024;  // 2000MB/s
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
  rate_limiter_auto_tuned_ = at == "yes" || at.empty();

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
  
  // disable_auto_compactions
  GetConfBool("disable_auto_compactions", &disable_auto_compactions_);

  small_compaction_threshold_ = 5000;
  GetConfInt("small-compaction-threshold", &small_compaction_threshold_);
  if (small_compaction_threshold_ < 0) {
    small_compaction_threshold_ = 0;
  } else if (small_compaction_threshold_ >= 100000) {
    small_compaction_threshold_ = 100000;
  }

  small_compaction_duration_threshold_ = 10000;
  GetConfInt("small-compaction-duration-threshold", &small_compaction_duration_threshold_);
  if (small_compaction_duration_threshold_ < 0) {
    small_compaction_duration_threshold_ = 0;
  } else if (small_compaction_duration_threshold_ >= 1000000) {
    small_compaction_duration_threshold_ = 1000000;
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

  max_background_jobs_ = (1 + 2);
  GetConfInt("max-background-jobs", &max_background_jobs_);
  if (max_background_jobs_ <= 0) {
    max_background_jobs_ = (1 + 2);
  }
  if (max_background_jobs_ >= (8 + 4)) {
    max_background_jobs_ = (8 + 4);
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
  share_block_cache_ = sbc == "yes";

  std::string ciafb;
  GetConfStr("cache-index-and-filter-blocks", &ciafb);
  cache_index_and_filter_blocks_ = ciafb == "yes";

  std::string plfaibic;
  GetConfStr("pin_l0_filter_and_index_blocks_in_cache", &plfaibic);
  pin_l0_filter_and_index_blocks_in_cache_ = plfaibic == "yes";

  std::string offh;
  GetConfStr("optimize-filters-for-hits", &offh);
  optimize_filters_for_hits_ = offh == "yes";

  std::string lcdlb;
  GetConfStr("level-compaction-dynamic-level-bytes", &lcdlb);
  level_compaction_dynamic_level_bytes_ = lcdlb == "yes" || lcdlb.empty();

  // daemonize
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ = dmz == "yes";

  // binlog
  std::string wb;
  GetConfStr("write-binlog", &wb);
  write_binlog_ = wb != "no";
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

  // acl users
  GetConfStrMulti("user", &users_);

  GetConfStr("aclfile", &aclFile_);

  std::string acl_pubsub_default;
  GetConfStr("acl-pubsub-default", &acl_pubsub_default);
  if (acl_pubsub_default == "allchannels") {
    acl_pubsub_default_ = static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS);
  }

  int tmp_acllog_max_len = 128;
  GetConfInt("acllog-max-len", &tmp_acllog_max_len);
  if (tmp_acllog_max_len < 0) {
    tmp_acllog_max_len = 128;
  }
  acl_Log_max_len_ = tmp_acllog_max_len;

  // slaveof
  slaveof_ = "";
  GetConfStr("slaveof", &slaveof_);
  
  int cache_num = 16;
  GetConfInt("cache-num", &cache_num);
  cache_num_ = (0 >= cache_num || 48 < cache_num) ? 16 : cache_num;

  int cache_model = 0;
  GetConfInt("cache-model", &cache_model);
  cache_model_ = (PIKA_CACHE_NONE > cache_model || PIKA_CACHE_READ < cache_model) ? PIKA_CACHE_NONE : cache_model;

  std::string cache_type;
  GetConfStr("cache-type", &cache_type);
  SetCacheType(cache_type);

  int zset_cache_start_pos = 0;
  GetConfInt("zset-cache-start-direction", &zset_cache_start_pos);
  if (zset_cache_start_pos != cache::CACHE_START_FROM_BEGIN && zset_cache_start_pos != cache::CACHE_START_FROM_END) {
    zset_cache_start_pos = cache::CACHE_START_FROM_BEGIN;
  }
  zset_cache_start_pos_ = zset_cache_start_pos;

  int zset_cache_field_num_per_key = DEFAULT_CACHE_ITEMS_PER_KEY;
  GetConfInt("zset-cache-field-num-per-key", &zset_cache_field_num_per_key);
  if (zset_cache_field_num_per_key <= 0) {
    zset_cache_field_num_per_key = DEFAULT_CACHE_ITEMS_PER_KEY;
  }
  zset_cache_field_num_per_key_ = zset_cache_field_num_per_key;

  int64_t cache_maxmemory = PIKA_CACHE_SIZE_DEFAULT;
  GetConfInt64("cache-maxmemory", &cache_maxmemory);
  cache_maxmemory_ = (PIKA_CACHE_SIZE_MIN > cache_maxmemory) ? PIKA_CACHE_SIZE_DEFAULT : cache_maxmemory;

  int cache_maxmemory_policy = 1;
  GetConfInt("cache-maxmemory-policy", &cache_maxmemory_policy);
  cache_maxmemory_policy_ = (0 > cache_maxmemory_policy || 7 < cache_maxmemory_policy) ? 1 : cache_maxmemory_policy;

  int cache_maxmemory_samples = 5;
  GetConfInt("cache-maxmemory-samples", &cache_maxmemory_samples);
  cache_maxmemory_samples_ = (1 > cache_maxmemory_samples) ? 5 : cache_maxmemory_samples;

  int cache_lfu_decay_time = 1;
  GetConfInt("cache-lfu-decay-time", &cache_lfu_decay_time);
  cache_lfu_decay_time_ = (0 > cache_lfu_decay_time) ? 1 : cache_lfu_decay_time;
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

  // throttle-bytes-per-second
  GetConfInt("throttle-bytes-per-second", &throttle_bytes_per_second_);
  if (throttle_bytes_per_second_ <= 0) {
    throttle_bytes_per_second_ = 207200000;
  }

  GetConfInt("max-rsync-parallel-num", &max_rsync_parallel_num_);
  if (max_rsync_parallel_num_ <= 0) {
    max_rsync_parallel_num_ = 4;
  }

  return ret;
}

void PikaConf::TryPushDiffCommands(const std::string& command, const std::string& value) {
  if (!CheckConfExist(command)) {
    diff_commands_[command] = value;
  }
}

void PikaConf::SetCacheType(const std::string& value) {
  cache_string_ = cache_set_ = cache_zset_ = cache_hash_ = cache_list_ = cache_bit_ = 0;
  if (value == "") {
    return;
  }
  std::lock_guard l(rwlock_);

  std::string lower_value = value;
  pstd::StringToLower(lower_value);
  lower_value.erase(remove_if(lower_value.begin(), lower_value.end(), isspace), lower_value.end());
  pstd::StringSplit(lower_value, COMMA, cache_type_);
  for (auto& type : cache_type_) {
    if (type == "string") {
      cache_string_ = 1;
    } else if (type == "set") {
      cache_set_ = 1;
    } else if (type == "zset") {
      cache_zset_ = 1;
    } else if (type == "hash") {
      cache_hash_ = 1;
    } else if (type == "list") {
      cache_list_ = 1;
    } else if (type == "bit") {
      cache_bit_ = 1;
    }
  }
}

int PikaConf::ConfigRewrite() {
  //  std::string userblacklist = suser_blacklist();

  std::lock_guard l(rwlock_);
  // Only set value for config item that can be config set.
  SetConfInt("timeout", timeout_);
  SetConfStr("requirepass", requirepass_);
  SetConfStr("masterauth", masterauth_);
  //  SetConfStr("userpass", userpass_);
  //  SetConfStr("userblacklist", userblacklist);
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
  SetConfStr("run-id", run_id_);
  SetConfStr("replication-id", replication_id_);
  SetConfInt("max-cache-statistic-keys", max_cache_statistic_keys_);
  SetConfInt("small-compaction-threshold", small_compaction_threshold_);
  SetConfInt("small-compaction-duration-threshold", small_compaction_duration_threshold_);
  SetConfInt("max-client-response-size", static_cast<int32_t>(max_client_response_size_));
  SetConfInt("db-sync-speed", db_sync_speed_);
  SetConfStr("compact-cron", compact_cron_);
  SetConfStr("compact-interval", compact_interval_);
  SetConfStr("disable_auto_compactions", disable_auto_compactions_ ? "true" : "false");
  SetConfInt64("least-free-disk-resume-size", least_free_disk_to_resume_);
  SetConfInt64("manually-resume-interval", resume_check_interval_);
  SetConfDouble("min-check-resume-ratio", min_check_resume_ratio_);
  SetConfInt("slave-priority", slave_priority_);
  SetConfInt("throttle-bytes-per-second", throttle_bytes_per_second_);
  SetConfInt("max-rsync-parallel-num", max_rsync_parallel_num_);
  SetConfInt("sync-window-size", sync_window_size_.load());
  SetConfInt("consensus-level", consensus_level_.load());
  SetConfInt("replication-num", replication_num_.load());
  // options for storage engine
  SetConfInt("max-cache-files", max_cache_files_);
  SetConfInt("max-background-compactions", max_background_compactions_);
  SetConfInt("max-background-jobs", max_background_jobs_);
  SetConfInt("max-write-buffer-num", max_write_buffer_num_);
  SetConfInt64("write-buffer-size", write_buffer_size_);
  SetConfInt64("arena-block-size", arena_block_size_);
  SetConfInt64("slotmigrate", slotmigrate_);
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
  return static_cast<int>(WriteBack());
}

int PikaConf::ConfigRewriteReplicationID() {
  std::lock_guard l(rwlock_);
  SetConfStr("replication-id", replication_id_);
  if (!diff_commands_.empty()) {
    std::vector<pstd::BaseConf::Rep::ConfItem> filtered_items;
    for (const auto& diff_command : diff_commands_) {
      if (!diff_command.second.empty()) {
        pstd::BaseConf::Rep::ConfItem item(pstd::BaseConf::Rep::kConf, diff_command.first, diff_command.second);
        filtered_items.push_back(item);
      }
    }
    if (!filtered_items.empty()) {
      pstd::BaseConf::Rep::ConfItem comment_item(pstd::BaseConf::Rep::kComment,
                                                 "# Generated by ReplicationID CONFIG REWRITE\n");
      PushConfItem(comment_item);
      for (const auto& item : filtered_items) {
        PushConfItem(item);
      }
    }
    diff_commands_.clear();
  }
  return static_cast<int>(WriteBack());
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
