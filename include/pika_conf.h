// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CONF_H_
#define PIKA_CONF_H_
#include <pthread.h>
#include <stdlib.h>
#include <string>
#include <vector>

#include "slash/include/base_conf.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/slash_string.h"
#include "slash/include/xdebug.h"
#include "include/pika_define.h"

typedef slash::RWLock RWLock;

// global class, class members well initialized
class PikaConf : public slash::BaseConf {
 public:
  PikaConf(const std::string& path);
  ~PikaConf()             { pthread_rwlock_destroy(&rwlock_); }

  // Getter
  int port()              { RWLock l(&rwlock_, false); return port_; }
  std::string slaveof() {RWLock l(&rwlock_, false); return slaveof_;}
  int slave_priority() {RWLock l(&rwlock_, false); return slave_priority_;}
  bool write_binlog() {RWLock l(&rwlock_, false); return write_binlog_;}
  int thread_num()        { RWLock l(&rwlock_, false); return thread_num_; }
  int thread_pool_size()       { RWLock l(&rwlock_, false); return thread_pool_size_; }
  int sync_thread_num()        { RWLock l(&rwlock_, false); return sync_thread_num_; }
  int sync_buffer_size()        { RWLock l(&rwlock_, false); return sync_buffer_size_; }
  std::string log_path()  { RWLock l(&rwlock_, false); return log_path_; }
  int log_level()         { RWLock l(&rwlock_, false); return log_level_; }
  std::string db_path()   { RWLock l(&rwlock_, false); return db_path_; }
  std::string db_sync_path()   { RWLock l(&rwlock_, false); return db_sync_path_; }
  int db_sync_speed()   { RWLock l(&rwlock_, false); return db_sync_speed_; }
  std::string compact_cron() { RWLock l(&rwlock_, false); return compact_cron_; }
  std::string compact_interval() { RWLock l(&rwlock_, false); return compact_interval_; }
  int64_t write_buffer_size() { RWLock l(&rwlock_, false); return write_buffer_size_; }
  int timeout()           { RWLock l(&rwlock_, false); return timeout_; }
  std::string server_id() { RWLock l(&rwlock_, false); return server_id_; }

  std::string requirepass()     { RWLock l(&rwlock_, false); return requirepass_; }
  std::string masterauth()     { RWLock l(&rwlock_, false); return masterauth_; }
  bool slotmigrate()     { RWLock l(&rwlock_, false); return slotmigrate_; }
  std::string bgsave_path()     { RWLock l(&rwlock_, false); return bgsave_path_; }
  int expire_dump_days() { RWLock l(&rwlock_, false); return expire_dump_days_; }
  std::string bgsave_prefix()     { RWLock l(&rwlock_, false); return bgsave_prefix_; }
  std::string userpass()        { RWLock l(&rwlock_, false); return userpass_; }
  const std::string suser_blacklist() {
    RWLock l(&rwlock_, false);
    return slash::StringConcat(user_blacklist_, COMMA);
  }
  const std::vector<std::string>& vuser_blacklist() {
    RWLock l(&rwlock_, false); return user_blacklist_;
  }
  std::string compression()     { RWLock l(&rwlock_, false); return compression_; }
  int target_file_size_base()   { RWLock l(&rwlock_, false); return target_file_size_base_; }
  int max_cache_statistic_keys() {RWLock l(&rwlock_, false); return max_cache_statistic_keys_;}
  int small_compaction_threshold() {RWLock l(&rwlock_, false); return small_compaction_threshold_;}
  int max_background_flushes()  { RWLock l(&rwlock_, false); return max_background_flushes_; }
  int max_background_compactions()   { RWLock l(&rwlock_, false); return max_background_compactions_; }
  int max_cache_files()          { RWLock l(&rwlock_, false); return max_cache_files_; }
  int max_bytes_for_level_multiplier() {RWLock l(&rwlock_, false); return max_bytes_for_level_multiplier_; }
  int64_t block_size() {RWLock l(&rwlock_, false); return block_size_; }
  int64_t block_cache() {RWLock l(&rwlock_, false); return block_cache_; }
  bool share_block_cache() {RWLock l(&rwlock_, false); return share_block_cache_; }
  bool cache_index_and_filter_blocks() {RWLock l(&rwlock_, false); return cache_index_and_filter_blocks_; }
  bool optimize_filters_for_hits() {RWLock l(&rwlock_, false); return optimize_filters_for_hits_; }
  bool level_compaction_dynamic_level_bytes() {RWLock l(&rwlock_, false); return level_compaction_dynamic_level_bytes_; }
  int expire_logs_nums()        { RWLock l(&rwlock_, false); return expire_logs_nums_; }
  int expire_logs_days()        { RWLock l(&rwlock_, false); return expire_logs_days_; }
  std::string conf_path()       { RWLock l(&rwlock_, false); return conf_path_; }
  bool slave_read_only()        { RWLock l(&rwlock_, false); return slave_read_only_; }
  int maxclients()           { RWLock l(&rwlock_, false); return maxclients_; }
  int root_connection_num()     { RWLock l(&rwlock_, false); return root_connection_num_; }
  bool slowlog_write_errorlog() { RWLock l(&rwlock_, false); return slowlog_write_errorlog_;}
  int slowlog_slower_than()     { RWLock l(&rwlock_, false); return slowlog_log_slower_than_; }
  int slowlog_max_len()         { RWLock L(&rwlock_, false); return slowlog_max_len_; }
  std::string network_interface() { RWLock l(&rwlock_, false); return network_interface_; }

  // Immutable config items, we don't use lock.
  bool daemonize()              { return daemonize_; }
  std::string pidfile()         { return pidfile_; }
  int binlog_file_size()        { return binlog_file_size_; }

  // Setter
  void SetPort(const int value)                 { RWLock l(&rwlock_, true); port_ = value; }
  void SetThreadNum(const int value)            { RWLock l(&rwlock_, true); thread_num_ = value; }
  void SetLogLevel(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("loglevel", value ? "ERROR" : "INFO");
    log_level_ = value;
  }
  void SetTimeout(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("timeout", std::to_string(value));
    timeout_ = value;
  }
  void SetThreadPoolSize(const int value)       { RWLock l(&rwlock_, true); thread_pool_size_ = value; }
  void SetSlaveof(const std::string value) {
    RWLock l(&rwlock_, true);
    slaveof_ = value;
  }
  void SetSlavePriority(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("slave-priority", std::to_string(value));
    slave_priority_ = value;
  }
  void SetWriteBinlog(const std::string& value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("write-binlog", value);
    write_binlog_ = (value == "yes") ? true : false;
  }
  void SetMaxCacheStatisticKeys(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("max-cache-statistic-keys", std::to_string(value));
    max_cache_statistic_keys_ = value;
  }
  void SetSmallCompactionThreshold(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("small-compaction-threshold", std::to_string(value));
    small_compaction_threshold_ = value;
  }
  void SetBgsavePath(const std::string &value) {
    RWLock l(&rwlock_, true);
    bgsave_path_ = value;
    if (value[value.length() - 1] != '/') {
      bgsave_path_ += "/";
    }
  }
  void SetExpireDumpDays(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("dump-expire", std::to_string(value));
    expire_dump_days_ = value;
  }
  void SetBgsavePrefix(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("dump-prefix", value);
    bgsave_prefix_ = value;
  }
  void SetRequirePass(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("requirepass", value);
    requirepass_ = value;
  }
  void SetMasterAuth(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("masterauth", value);
    masterauth_ = value;
  }
  void SetSlotMigrate(const std::string &value) {
    RWLock l(&rwlock_, true);
    slotmigrate_ =  (value == "yes") ? true : false;
  }
  void SetUserPass(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("userpass", value);
    userpass_ = value;
  }
  void SetUserBlackList(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("userblacklist", value);
    slash::StringSplit(value, COMMA, user_blacklist_);
    for (auto& item : user_blacklist_) {
      slash::StringToLower(item);
    }
  }
  void SetSlaveReadOnly(const bool value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("slave-read-only", value == true ? "yes" : "no");
    slave_read_only_ = value;
  }
  void SetExpireLogsNums(const int value){
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("expire-logs-nums", std::to_string(value));
    expire_logs_nums_ = value;
  }
  void SetExpireLogsDays(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("expire-logs-days", std::to_string(value));
    expire_logs_days_ = value;
  }
  void SetMaxConnection(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("maxclients", std::to_string(value));
    maxclients_ = value;
  }
  void SetRootConnectionNum(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("root-connection-num", std::to_string(value));
    root_connection_num_ = value;
  }
  void SetSlowlogWriteErrorlog(const bool value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("slowlog-write-errorlog", value == true ? "yes" : "no");
    slowlog_write_errorlog_ = value;
  }
  void SetSlowlogSlowerThan(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("slowlog-log-slower-than", std::to_string(value));
    slowlog_log_slower_than_ = value;
  }
  void SetSlowlogMaxLen(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("slowlog-max-len", std::to_string(value));
    slowlog_max_len_ = value;
  }
  void SetDbSyncSpeed(const int value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("db-sync-speed", std::to_string(value));
    db_sync_speed_ = value;
  }
  void SetCompactCron(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("compact-cron", value);
    compact_cron_ = value;
  }
  void SetCompactInterval(const std::string &value) {
    RWLock l(&rwlock_, true);
    TryPushDiffCommands("compact-interval", value);
    compact_interval_ = value;
  }

  int Load();
  int ConfigRewrite();

private:
  int port_;
  std::string slaveof_;
  int slave_priority_;
  int thread_num_;
  int thread_pool_size_;
  int sync_thread_num_;
  int sync_buffer_size_;
  std::string log_path_;
  std::string db_path_;
  std::string db_sync_path_;
  int expire_dump_days_;
  int db_sync_speed_;
  std::string compact_cron_;
  std::string compact_interval_;
  int64_t write_buffer_size_;
  int log_level_;
  bool daemonize_;
  bool slotmigrate_;
  int timeout_;
  std::string server_id_;
  std::string requirepass_;
  std::string masterauth_;
  std::string userpass_;
  std::vector<std::string> user_blacklist_;
  std::string bgsave_path_;
  std::string bgsave_prefix_;
  std::string pidfile_;

  //char pidfile_[PIKA_WORD_SIZE];
  std::string compression_;
  int maxclients_;
  int root_connection_num_;
  bool slowlog_write_errorlog_;
  int slowlog_log_slower_than_;
  int slowlog_max_len_;
  int expire_logs_days_;
  int expire_logs_nums_;
  bool slave_read_only_;
  std::string conf_path_;
  int max_cache_statistic_keys_;
  int small_compaction_threshold_;
  int max_background_flushes_;
  int max_background_compactions_;
  int max_cache_files_;
  int max_bytes_for_level_multiplier_;
  int64_t block_size_;
  int64_t block_cache_;
  bool share_block_cache_;
  bool cache_index_and_filter_blocks_;
  bool optimize_filters_for_hits_;
  bool level_compaction_dynamic_level_bytes_;

  std::string network_interface_;

  // diff commands between cached commands and config file commands
  std::map<std::string, std::string> diff_commands_;
  void TryPushDiffCommands(const std::string& command, const std::string& value);

  //char username_[30];
  //char password_[30];

  //
  // Critical configure items
  //
  bool write_binlog_;
  int target_file_size_base_;
  int binlog_file_size_;

  pthread_rwlock_t rwlock_;
};

#endif
