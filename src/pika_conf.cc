// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "sys/stat.h"
#include "include/pika_conf.h"
#include "glog/logging.h"

#include <fstream>

#include <iostream>

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
  if (timeout_ <= 0) {
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
  GetConfInt("slowlog-log-slower-than", &slowlog_log_slower_than_);
  std::string user_blacklist;
  GetConfStr("userblacklist", &user_blacklist);
  SetUserBlackList(std::string(user_blacklist));
  GetConfStr("dump-path", &bgsave_path_);
  if (bgsave_path_[bgsave_path_.length() - 1] != '/') {
    bgsave_path_ += "/";
  }
  GetConfInt("expire-dump-days", &expire_dump_days_);
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
  GetConfBool("slave-read-only", &readonly_);

  //
  // Immutable Sections
  //
  GetConfInt("port", &port_);
  GetConfStr("double-master-addr", &double_master_addr_);
  GetConfInt("double-master-sid", &double_master_sid_);
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
    std::string::size_type len = compact_cron_.length();
    std::string::size_type colon = compact_cron_.find("-");
    std::string::size_type underline = compact_cron_.find("/");
    if (colon == std::string::npos || underline == std::string::npos ||
        colon >= underline || colon + 1 >= len ||
        colon + 1 == underline || underline + 1 >= len) {
        compact_cron_ = "";
    } else {
      int start = std::atoi(compact_cron_.substr(0, colon).c_str());
      int end = std::atoi(compact_cron_.substr(colon+1, underline).c_str());
      int usage = std::atoi(compact_cron_.substr(underline+1).c_str());
      if (start < 0 || start > 23 || end < 0 || end > 23 || usage < 0 || usage > 100) {
        compact_cron_ = "";
      }
    }
  }

  // write_buffer_size
  GetConfInt("write-buffer-size", &write_buffer_size_);
  if (write_buffer_size_ <= 0 ) {
      write_buffer_size_ = 4194304; // 40M
  }

  // target_file_size_base
  GetConfInt("target-file-size-base", &target_file_size_base_);
  if (target_file_size_base_ <= 0) {
      target_file_size_base_ = 1048576; // 10M
  }

  max_background_flushes_ = 1;
  GetConfInt("max-background-flushes", &max_background_flushes_);
  if (max_background_flushes_ <= 0) {
    max_background_flushes_ = 1;
  }
  if (max_background_flushes_ >= 4) {
    max_background_flushes_ = 4;
  }

  max_background_compactions_ = 1;
  GetConfInt("max-background-compactions", &max_background_compactions_);
  if (max_background_compactions_ <= 0) {
    max_background_compactions_ = 1;
  }
  if (max_background_compactions_ >= 4) {
    max_background_compactions_ = 4;
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

  // daemonize
  std::string dmz;
  GetConfStr("daemonize", &dmz);
  daemonize_ =  (dmz == "yes") ? true : false;
  GetConfInt("binlog-file-size", &binlog_file_size_);
  if (binlog_file_size_ < 1024 || static_cast<int64_t>(binlog_file_size_) > (1024LL * 1024 * 1024)) {
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
  if (db_sync_speed_ < 0 || db_sync_speed_ > 125) {
    db_sync_speed_ = 125;
  }
  // network interface
  network_interface_ = "";
  GetConfStr("network-interface", &network_interface_);

  // slaveof
  slaveof_ = "";
  GetConfStr("slaveof", &slaveof_);
  return ret;
}

int PikaConf::ConfigRewrite() {
  SetConfInt("port", port_);
  SetConfInt("thread-num", thread_num_);
  SetConfInt("sync-thread-num", sync_thread_num_);
  SetConfInt("sync-buffer-size", sync_buffer_size_);
  SetConfStr("log-path", log_path_);
  SetConfStr("loglevel", log_level_ ? "ERROR" : "INFO");
  SetConfStr("db-path", db_path_);
  SetConfStr("db-sync-path", db_sync_path_);
  SetConfInt("db-sync-speed", db_sync_speed_);
  SetConfInt("write-buffer-size", write_buffer_size_);
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
  SetConfInt("expire-dump-days", expire_dump_days_);
  SetConfStr("pidfile", pidfile_);
  SetConfInt("maxclients", maxclients_);
  SetConfInt("target-file-size-base", target_file_size_base_);
  SetConfInt("expire-logs-days", expire_logs_days_);
  SetConfInt("expire-logs-nums", expire_logs_nums_);
  SetConfInt("root-connection-num", root_connection_num_);
  SetConfInt("slowlog-log-slower-than", slowlog_log_slower_than_);
  SetConfBool("slave-read-only", readonly_);
  SetConfStr("compact-cron", compact_cron_);
  SetConfStr("network-interface", network_interface_);
  SetConfStr("slaveof", slaveof_);

  SetConfInt("binlog-file-size", binlog_file_size_);
  SetConfStr("compression", compression_);
  SetConfInt("max-background-flushes", max_background_flushes_);
  SetConfInt("max-background-compactions", max_background_compactions_);
  SetConfInt("max-cache-files", max_cache_files_);
  SetConfInt("max-bytes-for-level-multiplier", max_bytes_for_level_multiplier_);

  return WriteBack();
}
