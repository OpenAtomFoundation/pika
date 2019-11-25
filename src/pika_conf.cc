// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "sys/stat.h"
#include "include/pika_conf.h"
#include "glog/logging.h"

#include <fstream>

#include <iostream>
#include <algorithm>

PikaConf::PikaConf(const std::string& path):
  slash::BaseConf(path), conf_path_(path)
{
  pthread_rwlock_init(&rwlock_, NULL);
}

int PikaConf::Load()
{
  int ret = LoadConf();
  if (ret != 0) {
    return ret;
  }

  // Mutable Section
  std::string loglevel;
  GetConfStr("loglevel", &loglevel);
  slash::StringToLower(loglevel);
  if (loglevel == "info") {
    SetLogLevel(0);
  } else if (loglevel == "error") {
    SetLogLevel(1);
  } else {
    SetLogLevel(0);
    fprintf(stderr, "Invalid loglevel value in conf file, only INFO or ERROR\n");
    exit(-1);
  }
  GetConfInt("timeout", &timeout_);
  if (timeout_ < 0) {
      timeout_ = 60; // 60s
  }
  GetConfStr("server-id", &server_id_);
  if (server_id_.empty()) {
    server_id_ = "1";
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
  slowlog_write_errorlog_ = swe == "yes" ? true : false;

  GetConfInt("slowlog-log-slower-than", &slowlog_log_slower_than_);
  GetConfInt("slowlog-max-len", &slowlog_max_len_);
  if (slowlog_max_len_ == 0) {
    slowlog_max_len_ = 128;
  }
  std::string user_blacklist;
  GetConfStr("userblacklist", &user_blacklist);
  slash::StringSplit(user_blacklist, COMMA, user_blacklist_);
  for (auto& item : user_blacklist_) {
    slash::StringToLower(item);
  }
  GetConfStr("dump-path", &bgsave_path_);
  if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
    bgsave_path_ += "/";
  }
  GetConfInt("dump-expire", &expire_dump_days_);
  if (expire_dump_days_ < 0 ) {
      expire_dump_days_ = 0;
  }
  GetConfStr("dump-prefix", &bgsave_prefix_);

  GetConfInt("expire-logs-nums", &expire_logs_nums_);
  if (expire_logs_nums_ <= 10 ) {
      expire_logs_nums_ = 10;
  }
  GetConfInt("expire-logs-days", &expire_logs_days_);
  if (expire_logs_days_ <= 0 ) {
      expire_logs_days_ = 1;
  }
  GetConfStr("compression", &compression_);
  slave_read_only_ = true;
  GetConfBool("slave-read-only", &slave_read_only_);
  GetConfInt("slave-priority", &slave_priority_);

  //
  // Immutable Sections
  //
  GetConfInt("port", &port_);
  GetConfStr("double-master-ip", &double_master_ip_);
  GetConfInt("double-master-port", &double_master_port_);
  GetConfStr("double-master-server-id", &double_master_sid_);
  if (double_master_sid_.empty()) {
    double_master_sid_ = "0";
  }
  GetConfStr("log-path", &log_path_);
  GetConfStr("db-path", &db_path_);
  if (log_path_[log_path_.length() - 1] != '/') {
    log_path_ += "/";
  }
  GetConfInt("thread-num", &thread_num_);
  if (thread_num_ <= 0) {
    thread_num_ = 12;
  }
  if (thread_num_ > 24) {
    thread_num_ = 24;
  }
  GetConfInt("thread-pool-size", &thread_pool_size_);
  if (thread_pool_size_ <= 0) {
    thread_pool_size_ = 8;
  }
  if (thread_pool_size_ > 24) {
    thread_pool_size_ = 24;
  }
  GetConfInt("sync-thread-num", &sync_thread_num_);
  if (sync_thread_num_ <= 0) {
    sync_thread_num_ = 3;
  }
  if (sync_thread_num_ > 24) {
    sync_thread_num_ = 24;
  }
  GetConfInt("sync-buffer-size", &sync_buffer_size_);
  if (sync_buffer_size_ <= 0) {
    sync_buffer_size_ = 5;
  } else if (sync_buffer_size_ > 100) {
    sync_buffer_size_ = 100;
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
    if (colon == std::string::npos || underline == std::string::npos ||
        colon >= underline || colon + 1 >= len ||
        colon + 1 == underline || underline + 1 >= len) {
        compact_cron_ = "";
    } else {
      int week = std::atoi(week_str.c_str());
      int start = std::atoi(compact_cron.substr(0, colon).c_str());
      int end = std::atoi(compact_cron.substr(colon + 1, underline).c_str());
      int usage = std::atoi(compact_cron.substr(underline + 1).c_str());
      if ((have_week && (week < 1 || week > 7)) || start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 || usage > 100) {
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
      int usage = std::atoi(compact_interval_.substr(slash+1).c_str());
      if (interval <= 0 || usage < 0 || usage > 100) {
        compact_interval_ = "";
      }
    }
  }

  // write_buffer_size
  GetConfInt64("write-buffer-size", &write_buffer_size_);
  if (write_buffer_size_ <= 0 ) {
      write_buffer_size_ = 4194304; // 40M
  }

  // target_file_size_base
  GetConfInt("target-file-size-base", &target_file_size_base_);
  if (target_file_size_base_ <= 0) {
      target_file_size_base_ = 1048576; // 10M
  }

  max_cache_statistic_keys_ = 0;
  GetConfInt("max-cache-statistic-keys", &max_cache_statistic_keys_);
  if (max_cache_statistic_keys_ <= 0) {
    max_cache_statistic_keys_ = 0;
  }

  small_compaction_threshold_ = 5000;
  GetConfInt("small-compaction-threshold", &small_compaction_threshold_);
  if (small_compaction_threshold_ <= 0
    || small_compaction_threshold_ >= 100000) {
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
  GetConfInt64("block-size", &block_size_);
  if (block_size_ <= 0) {
    block_size_ = 4 * 1024;
  }

  block_cache_ = 8 * 1024 * 1024;
  GetConfInt64("block-cache", &block_cache_);
  if (block_cache_ < 0) {
    block_cache_ = 8 * 1024 * 1024;
  }

  std::string sbc;
  GetConfStr("share-block-cache", &sbc);
  share_block_cache_ = (sbc == "yes") ? true : false;

  std::string ciafb;
  GetConfStr("cache-index-and-filter-blocks", &ciafb);
  cache_index_and_filter_blocks_ = (ciafb == "yes") ? true : false;

  std::string offh;
  GetConfStr("optimize-filters-for-hits", &offh);
  optimize_filters_for_hits_ = (offh == "yes") ? true : false;

  std::string lcdlb;
  GetConfStr("level-compaction-dynamic-level-bytes", &lcdlb);
  level_compaction_dynamic_level_bytes_ = (lcdlb == "yes") ? true : false;

  // daemonize
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ =  (dmz == "yes") ? true : false;

  // binlog
  std::string wb;
  GetConfStr("write-binlog", &wb);
  write_binlog_ = (wb == "no") ? false : true;
  GetConfStr("identify-binlog-type", &identify_binlog_type_);
  if (identify_binlog_type_ != "new" && identify_binlog_type_ != "old") {
    identify_binlog_type_ = "new";
  }
  GetConfInt("binlog-file-size", &binlog_file_size_);
  if (binlog_file_size_ < 1024
    || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024)) {
    binlog_file_size_ = 100 * 1024 * 1024;    // 100M
  }
  GetConfStr("pidfile", &pidfile_);

  // slot migrate
  std::string smgrt;
  GetConfStr("slotmigrate", &smgrt);
  slotmigrate_ =  (smgrt == "yes") ? true : false;

  // db sync
  GetConfStr("db-sync-path", &db_sync_path_);
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
  return ret;
}

void PikaConf::TryPushDiffCommands(const std::string& command, const std::string& value) {
  if (!CheckConfExist(command)) {
    diff_commands_[command] = value;
  }
}

int PikaConf::ConfigRewrite() {
  {
  RWLock l(&rwlock_, false);

  SetConfInt("port", port_);
  SetConfInt("thread-num", thread_num_);
  SetConfInt("thread-pool-size", thread_pool_size_);
  SetConfInt("sync-thread-num", sync_thread_num_);
  SetConfInt("sync-buffer-size", sync_buffer_size_);
  SetConfStr("log-path", log_path_);
  SetConfStr("loglevel", log_level_ ? "ERROR" : "INFO");
  SetConfStr("db-path", db_path_);
  SetConfStr("db-sync-path", db_sync_path_);
  SetConfInt("db-sync-speed", db_sync_speed_);
  SetConfInt64("write-buffer-size", write_buffer_size_);
  SetConfInt("timeout", timeout_);
  SetConfStr("server-id", server_id_);
  SetConfStr("requirepass", requirepass_);
  SetConfStr("masterauth", masterauth_);
  SetConfStr("userpass", userpass_);
  SetConfStr("userblacklist", suser_blacklist());
  SetConfStr("dump-prefix", bgsave_prefix_);
  SetConfStr("daemonize", daemonize_ ? "yes" : "no");
  SetConfStr("slotmigrate", slotmigrate_ ? "yes" : "no");
  SetConfStr("dump-path", bgsave_path_);
  SetConfInt("dump-expire", expire_dump_days_);
  SetConfStr("pidfile", pidfile_);
  SetConfInt("maxclients", maxclients_);
  SetConfInt("target-file-size-base", target_file_size_base_);
  SetConfInt("expire-logs-days", expire_logs_days_);
  SetConfInt("expire-logs-nums", expire_logs_nums_);
  SetConfInt("root-connection-num", root_connection_num_);
  SetConfStr("slowlog-write-errorlog", slowlog_write_errorlog_ ? "yes" : "no");
  SetConfInt("slowlog-log-slower-than", slowlog_log_slower_than_);
  SetConfInt("slowlog-max-len", slowlog_max_len_);
  SetConfStr("slave-read-only", slave_read_only_ ? "yes" : "no");
  SetConfStr("compact-cron", compact_cron_);
  SetConfStr("compact-interval", compact_interval_);
  SetConfStr("network-interface", network_interface_);
  SetConfStr("slaveof", slaveof_);
  SetConfInt("slave-priority", slave_priority_);

  SetConfStr("write-binlog", write_binlog_ ? "yes" : "no");
  SetConfInt("binlog-file-size", binlog_file_size_);
  SetConfStr("identify-binlog-type", identify_binlog_type_);
  SetConfStr("compression", compression_);
  SetConfInt("max-cache-statistic-keys", max_cache_statistic_keys_);
  SetConfInt("small-compaction-threshold", small_compaction_threshold_);
  SetConfInt("max-background-flushes", max_background_flushes_);
  SetConfInt("max-background-compactions", max_background_compactions_);
  SetConfInt("max-cache-files", max_cache_files_);
  SetConfInt("max-bytes-for-level-multiplier", max_bytes_for_level_multiplier_);
  SetConfInt64("block-size", block_size_);
  SetConfInt64("block-cache", block_cache_);
  SetConfStr("share-block-cache", share_block_cache_ ? "yes" : "no");
  SetConfStr("cache-index-and-filter-blocks", cache_index_and_filter_blocks_ ? "yes" : "no");
  SetConfStr("optimize-filters-for-hits", optimize_filters_for_hits_ ? "yes" : "no");
  SetConfStr("level-compaction-dynamic-level-bytes", level_compaction_dynamic_level_bytes_ ? "yes" : "no");
  }

  if (!diff_commands_.empty()) {
    std::vector<slash::BaseConf::Rep::ConfItem> filtered_items;
    for (const auto& diff_command : diff_commands_) {
      if (!diff_command.second.empty()) {
        slash::BaseConf::Rep::ConfItem item(slash::BaseConf::Rep::kConf, diff_command.first, diff_command.second);
        filtered_items.push_back(item);
      }
    }
    if (!filtered_items.empty()) {
      slash::BaseConf::Rep::ConfItem comment_item(slash::BaseConf::Rep::kComment, "# Generated by CONFIG REWRITE\n");
      PushConfItem(comment_item);
      for (const auto& item : filtered_items) {
        PushConfItem(item);
      }
    }
    RWLock l(&rwlock_, true);
    diff_commands_.clear();
  }
  return WriteBack();
}
