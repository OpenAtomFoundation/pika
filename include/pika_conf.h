// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CONF_H_
#define PIKA_CONF_H_

#include <atomic>
#include <map>
#include <set>
#include <unordered_set>

#include "rocksdb/compression_type.h"

#include "pstd/include/base_conf.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_string.h"

#include "acl.h"
#include "include/pika_define.h"
#include "rocksdb/compression_type.h"

#define kBinlogReadWinDefaultSize 9000
#define kBinlogReadWinMaxSize 90000
const uint32_t configRunIDSize = 40;
const uint32_t configReplicationIDSize = 50;

// global class, class members well initialized
class PikaConf : public pstd::BaseConf {
 public:
  PikaConf(const std::string& path);
  ~PikaConf() override = default;

  // Getter
  int port() {
    std::shared_lock l(rwlock_);
    return port_;
  }
  std::string slaveof() {
    std::shared_lock l(rwlock_);
    return slaveof_;
  }
  int slave_priority() {
    std::shared_lock l(rwlock_);
    return slave_priority_;
  }
  bool write_binlog() {
    std::shared_lock l(rwlock_);
    return write_binlog_;
  }
  int thread_num() {
    std::shared_lock l(rwlock_);
    return thread_num_;
  }
  int thread_pool_size() {
    std::shared_lock l(rwlock_);
    return thread_pool_size_;
  }
  int slow_cmd_thread_pool_size() {
    std::shared_lock l(rwlock_);
    return slow_cmd_thread_pool_size_;
  }
  int sync_thread_num() {
    std::shared_lock l(rwlock_);
    return sync_thread_num_;
  }
  std::string log_path() {
    std::shared_lock l(rwlock_);
    return log_path_;
  }
  std::string log_level() {
    std::shared_lock l(rwlock_);
    return log_level_;
  }
  std::string db_path() {
    std::shared_lock l(rwlock_);
    return db_path_;
  }
  int db_instance_num() {
    return db_instance_num_;
  }
  uint64_t rocksdb_ttl_second() {
    return rocksdb_ttl_second_.load();
  }
  uint64_t rocksdb_periodic_compaction_second() {
    return rocksdb_periodic_second_.load();
  }
  std::string db_sync_path() {
    std::shared_lock l(rwlock_);
    return db_sync_path_;
  }
  int db_sync_speed() {
    std::shared_lock l(rwlock_);
    return db_sync_speed_;
  }
  std::string compact_cron() {
    std::shared_lock l(rwlock_);
    return compact_cron_;
  }
  std::string compact_interval() {
    std::shared_lock l(rwlock_);
    return compact_interval_;
  }
  int max_subcompactions() {
    std::shared_lock l(rwlock_);
    return max_subcompactions_;
  }
  bool disable_auto_compactions() {
    std::shared_lock l(rwlock_);
    return disable_auto_compactions_;
  }
  int64_t least_resume_free_disk_size() {
    std::shared_lock l(rwlock_);
    return least_free_disk_to_resume_;
  }
  int64_t resume_interval() {
    std::shared_lock l(rwlock_);
    return resume_check_interval_;
  }
  double min_check_resume_ratio() {
    std::shared_lock l(rwlock_);
    return min_check_resume_ratio_;
  }
  int64_t write_buffer_size() {
    std::shared_lock l(rwlock_);
    return write_buffer_size_;
  }
  int min_write_buffer_number_to_merge() {
    std::shared_lock l(rwlock_);
    return min_write_buffer_number_to_merge_;
  }
  int level0_stop_writes_trigger() {
    std::shared_lock l(rwlock_);
    return level0_stop_writes_trigger_;
  }
  int level0_slowdown_writes_trigger() {
    std::shared_lock l(rwlock_);
    return level0_slowdown_writes_trigger_;
  }
  int level0_file_num_compaction_trigger() {
    std::shared_lock l(rwlock_);
    return level0_file_num_compaction_trigger_;
  }
  int64_t arena_block_size() {
    std::shared_lock l(rwlock_);
    return arena_block_size_;
  }
  int64_t slotmigrate_thread_num() {
    std::shared_lock l(rwlock_);
    return slotmigrate_thread_num_;
  }
  int64_t thread_migrate_keys_num() {
    std::shared_lock l(rwlock_);
    return thread_migrate_keys_num_;
  }
  int64_t max_write_buffer_size() {
    std::shared_lock l(rwlock_);
    return max_write_buffer_size_;
  }
  int max_write_buffer_number() {
    std::shared_lock l(rwlock_);
    return max_write_buffer_num_;
  }
  uint64_t MaxTotalWalSize() {
    std::shared_lock l(rwlock_);
    return max_total_wal_size_;
  }
  int64_t max_client_response_size() {
    std::shared_lock l(rwlock_);
    return max_client_response_size_;
  }
  int timeout() {
    std::shared_lock l(rwlock_);
    return timeout_;
  }
  int binlog_writer_num() {
    std::shared_lock l(rwlock_);
    return binlog_writer_num_;
  }
  bool slotmigrate() {
    std::shared_lock l(rwlock_);
    return slotmigrate_;
  }
  std::string server_id() {
    std::shared_lock l(rwlock_);
    return server_id_;
  }
  std::string run_id() {
    std::shared_lock l(rwlock_);
    return run_id_;
  }
  std::string replication_id() {
    std::shared_lock l(rwlock_);
    return replication_id_;
  }
  std::string requirepass() {
    std::shared_lock l(rwlock_);
    return requirepass_;
  }
  std::string masterauth() {
    std::shared_lock l(rwlock_);
    return masterauth_;
  }
  std::string userpass() {
    std::shared_lock l(rwlock_);
    return userpass_;
  }
  std::string bgsave_path() {
    std::shared_lock l(rwlock_);
    return bgsave_path_;
  }
  int expire_dump_days() {
    std::shared_lock l(rwlock_);
    return expire_dump_days_;
  }
  std::string bgsave_prefix() {
    std::shared_lock l(rwlock_);
    return bgsave_prefix_;
  }
  std::string user_blacklist_string() {
    std::shared_lock l(rwlock_);
    return pstd::StringConcat(user_blacklist_, COMMA);
  }
  const std::vector<std::string>& user_blacklist_vector() {
    std::shared_lock l(rwlock_);
    return user_blacklist_;
  }
  bool classic_mode() { return classic_mode_.load(); }
  int databases() {
    std::shared_lock l(rwlock_);
    return databases_;
  }
  int default_slot_num() {
    std::shared_lock l(rwlock_);
    return default_slot_num_;
  }
  const std::vector<DBStruct>& db_structs() {
    std::shared_lock l(rwlock_);
    return db_structs_;
  }
  std::string default_db() {
    std::shared_lock l(rwlock_);
    return default_db_;
  }
  std::string compression() {
    std::shared_lock l(rwlock_);
    return compression_;
  }
  int target_file_size_base() {
    std::shared_lock l(rwlock_);
    return target_file_size_base_;
  }
  int max_cache_statistic_keys() {
    std::shared_lock l(rwlock_);
    return max_cache_statistic_keys_;
  }
  int small_compaction_threshold() {
    std::shared_lock l(rwlock_);
    return small_compaction_threshold_;
  }
  int small_compaction_duration_threshold() {
    std::shared_lock l(rwlock_);
    return small_compaction_duration_threshold_;
  }
  int max_background_flushes() {
    std::shared_lock l(rwlock_);
    return max_background_flushes_;
  }
  int max_background_compactions() {
    std::shared_lock l(rwlock_);
    return max_background_compactions_;
  }
  int max_background_jobs() {
    std::shared_lock l(rwlock_);
    return max_background_jobs_;
  }
  int max_cache_files() {
    std::shared_lock l(rwlock_);
    return max_cache_files_;
  }
  int max_bytes_for_level_multiplier() {
    std::shared_lock l(rwlock_);
    return max_bytes_for_level_multiplier_;
  }
  int64_t block_size() {
    std::shared_lock l(rwlock_);
    return block_size_;
  }
  int64_t block_cache() {
    std::shared_lock l(rwlock_);
    return block_cache_;
  }
  int64_t num_shard_bits() {
    std::shared_lock l(rwlock_);
    return num_shard_bits_;
  }
  bool share_block_cache() {
    std::shared_lock l(rwlock_);
    return share_block_cache_;
  }
  bool enable_partitioned_index_filters() {
    std::shared_lock l(rwlock_);
    return enable_partitioned_index_filters_;
  }
  bool cache_index_and_filter_blocks() {
    std::shared_lock l(rwlock_);
    return cache_index_and_filter_blocks_;
  }
  bool pin_l0_filter_and_index_blocks_in_cache() {
    std::shared_lock l(rwlock_);
    return pin_l0_filter_and_index_blocks_in_cache_;
  }
  bool optimize_filters_for_hits() {
    std::shared_lock l(rwlock_);
    return optimize_filters_for_hits_;
  }
  bool level_compaction_dynamic_level_bytes() {
    std::shared_lock l(rwlock_);
    return level_compaction_dynamic_level_bytes_;
  }
  int expire_logs_nums() {
    std::shared_lock l(rwlock_);
    return expire_logs_nums_;
  }
  int expire_logs_days() {
    std::shared_lock l(rwlock_);
    return expire_logs_days_;
  }
  std::string conf_path() {
    std::shared_lock l(rwlock_);
    return conf_path_;
  }
  bool slave_read_only() {
    std::shared_lock l(rwlock_);
    return slave_read_only_;
  }
  int maxclients() {
    std::shared_lock l(rwlock_);
    return maxclients_;
  }
  int root_connection_num() {
    std::shared_lock l(rwlock_);
    return root_connection_num_;
  }
  bool slowlog_write_errorlog() { return slowlog_write_errorlog_.load(); }
  int slowlog_slower_than() { return slowlog_log_slower_than_.load(); }
  int slowlog_max_len() {
    std::shared_lock l(rwlock_);
    return slowlog_max_len_;
  }
  std::string network_interface() {
    std::shared_lock l(rwlock_);
    return network_interface_;
  }
  int cache_mode() { return cache_mode_; }
  int sync_window_size() { return sync_window_size_.load(); }
  int max_conn_rbuf_size() { return max_conn_rbuf_size_.load(); }
  int consensus_level() { return consensus_level_.load(); }
  int replication_num() { return replication_num_.load(); }
  int rate_limiter_mode() {
    std::shared_lock l(rwlock_);
    return rate_limiter_mode_;
  }
  int64_t rate_limiter_bandwidth() {
    std::shared_lock l(rwlock_);
    return rate_limiter_bandwidth_;
  }
  int64_t rate_limiter_refill_period_us() {
    std::shared_lock l(rwlock_);
    return rate_limiter_refill_period_us_;
  }
  int64_t rate_limiter_fairness() {
    std::shared_lock l(rwlock_);
    return rate_limiter_fairness_;
  }
  bool rate_limiter_auto_tuned() {
    std::shared_lock l(rwlock_);
    return rate_limiter_auto_tuned_;
  }
  bool IsCacheDisabledTemporarily() { return tmp_cache_disable_flag_; }
  int GetCacheString() { return cache_string_; }
  int GetCacheSet() { return cache_set_; }
  int GetCacheZset() { return cache_zset_; }
  int GetCacheHash() { return cache_hash_; }
  int GetCacheList() { return cache_list_; }
  int GetCacheBit() { return cache_bit_; }
  int GetCacheNum() { return cache_num_; }
  void SetCacheNum(const int value) { cache_num_ = value; }
  void SetCacheMode(const int value) { cache_mode_ = value; }
  void SetCacheStartDirection(const int value) { zset_cache_start_direction_ = value; }
  void SetCacheItemsPerKey(const int value) { zset_cache_field_num_per_key_ = value; }
  void SetCacheMaxmemory(const int64_t value) { cache_maxmemory_ = value; }
  void SetCacheMaxmemoryPolicy(const int value) { cache_maxmemory_policy_ = value; }
  void SetCacheMaxmemorySamples(const int value) { cache_maxmemory_samples_ = value; }
  void SetCacheLFUDecayTime(const int value) { cache_lfu_decay_time_ = value; }
  void UnsetCacheDisableFlag() { tmp_cache_disable_flag_ = false; }
  bool enable_blob_files() { return enable_blob_files_; }
  int64_t min_blob_size() { return min_blob_size_; }
  int64_t blob_file_size() { return blob_file_size_; }
  std::string blob_compression_type() { return blob_compression_type_; }
  bool enable_blob_garbage_collection() { return enable_blob_garbage_collection_; }
  double blob_garbage_collection_age_cutoff() { return blob_garbage_collection_age_cutoff_; }
  double blob_garbage_collection_force_threshold() { return blob_garbage_collection_force_threshold_; }
  int64_t blob_cache() { return blob_cache_; }
  int64_t blob_num_shard_bits() { return blob_num_shard_bits_; }

  // Rsync Rate limiting configuration
  int throttle_bytes_per_second() {
    std::shared_lock l(rwlock_);
    return throttle_bytes_per_second_;
  }
  int max_rsync_parallel_num() {
    std::shared_lock l(rwlock_);
    return max_rsync_parallel_num_;
  }
  int64_t rsync_timeout_ms() {
      return rsync_timeout_ms_.load(std::memory_order::memory_order_relaxed);
  }
  // Slow Commands configuration
  const std::string GetSlowCmd() {
    std::shared_lock l(rwlock_);
    return pstd::Set2String(slow_cmd_set_, ',');
  }

  const std::string GetUserBlackList() {
    std::shared_lock l(rwlock_);
    return userblacklist_;
  }

  bool is_slow_cmd(const std::string& cmd) {
    std::shared_lock l(rwlock_);
    return slow_cmd_set_.find(cmd) != slow_cmd_set_.end();
  }

  // Immutable config items, we don't use lock.
  bool daemonize() { return daemonize_; }
  std::string pidfile() { return pidfile_; }
  int binlog_file_size() { return binlog_file_size_; }
  std::vector<rocksdb::CompressionType> compression_per_level();
  std::string compression_all_levels() const { return compression_per_level_; };
  static rocksdb::CompressionType GetCompression(const std::string& value);

  std::vector<std::string>& users() { return users_; };
  std::string acl_file() { return aclFile_; };

  uint32_t acl_pubsub_default() { return acl_pubsub_default_.load(); }
  uint32_t acl_log_max_len() { return acl_Log_max_len_.load(); }

  // Setter
  void SetPort(const int value) {
    std::lock_guard l(rwlock_);
    port_ = value;
  }
  void SetThreadNum(const int value) {
    std::lock_guard l(rwlock_);
    thread_num_ = value;
  }
  void SetTimeout(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("timeout", std::to_string(value));
    timeout_ = value;
  }
  void SetThreadPoolSize(const int value) {
    std::lock_guard l(rwlock_);
    thread_pool_size_ = value;
  }

  void SetLowLevelThreadPoolSize(const int value) {
    std::lock_guard l(rwlock_);
    slow_cmd_thread_pool_size_ = value;
  }

  void SetSlaveof(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slaveof", value);
    slaveof_ = value;
  }

  void SetRocksdbTTLSecond(uint64_t ttl) {
    rocksdb_ttl_second_.store(ttl);
  }

  void SetRocksdbPeriodicSecond(uint64_t value) {
    rocksdb_periodic_second_.store(value);
  }

  void SetReplicationID(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("replication-id", value);
    replication_id_ = value;
  }
  void SetSlavePriority(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slave-priority", std::to_string(value));
    slave_priority_ = value;
  }
  void SetWriteBinlog(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("write-binlog", value);
    write_binlog_ = value == "yes";
  }
  void SetMaxCacheStatisticKeys(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-cache-statistic-keys", std::to_string(value));
    max_cache_statistic_keys_ = value;
  }
  void SetSmallCompactionThreshold(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("small-compaction-threshold", std::to_string(value));
    small_compaction_threshold_ = value;
  }
  void SetSmallCompactionDurationThreshold(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("small-compaction-duration-threshold", std::to_string(value));
    small_compaction_duration_threshold_ = value;
  }
  void SetMaxClientResponseSize(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-client-response-size", std::to_string(value));
    max_client_response_size_ = value;
  }
  void SetBgsavePath(const std::string& value) {
    std::lock_guard l(rwlock_);
    bgsave_path_ = value;
    if (value[value.length() - 1] != '/') {
      bgsave_path_ += "/";
    }
  }
  void SetExpireDumpDays(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("dump-expire", std::to_string(value));
    expire_dump_days_ = value;
  }
  void SetBgsavePrefix(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("dump-prefix", value);
    bgsave_prefix_ = value;
  }
  void SetRunID(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("run-id", value);
    run_id_ = value;
  }
  void SetRequirePass(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("requirepass", value);
    requirepass_ = value;
  }
  void SetMasterAuth(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("masterauth", value);
    masterauth_ = value;
  }
  void SetUserPass(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("userpass", value);
    userpass_ = value;
  }
  void SetUserBlackList(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("userblacklist", value);
    pstd::StringSplit(value, COMMA, user_blacklist_);
    for (auto& item : user_blacklist_) {
      pstd::StringToLower(item);
    }
  }
  void SetSlotMigrate(const bool value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slotmigrate", value ? "yes" : "no");
    slotmigrate_.store(value);
  }
  void SetSlotMigrateThreadNum(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slotmigrate-thread-num", std::to_string(value));
    slotmigrate_thread_num_ = value;
  }
  void SetThreadMigrateKeysNum(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("thread-migrate-keys-num", std::to_string(value));
    thread_migrate_keys_num_ = value;
  }
  void SetExpireLogsNums(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("expire-logs-nums", std::to_string(value));
    expire_logs_nums_ = value;
  }
  void SetExpireLogsDays(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("expire-logs-days", std::to_string(value));
    expire_logs_days_ = value;
  }
  void SetMaxConnection(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("maxclients", std::to_string(value));
    maxclients_ = value;
  }
  void SetRootConnectionNum(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("root-connection-num", std::to_string(value));
    root_connection_num_ = value;
  }
  void SetSlowlogWriteErrorlog(const bool value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slowlog-write-errorlog", value ? "yes" : "no");
    slowlog_write_errorlog_.store(value);
  }
  void SetSlowlogSlowerThan(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slowlog-log-slower-than", std::to_string(value));
    slowlog_log_slower_than_.store(value);
  }
  void SetSlowlogMaxLen(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("slowlog-max-len", std::to_string(value));
    slowlog_max_len_ = value;
  }
  void SetDbSyncSpeed(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("db-sync-speed", std::to_string(value));
    db_sync_speed_ = value;
  }
  void SetCompactCron(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("compact-cron", value);
    compact_cron_ = value;
  }
  void SetCompactInterval(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("compact-interval", value);
    compact_interval_ = value;
  }
  void SetDisableAutoCompaction(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("disable_auto_compactions", value);
    disable_auto_compactions_ = value == "true";
  }
  void SetMaxSubcompactions(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-subcompactions", std::to_string(value));
    max_subcompactions_ = value;
  }
  void SetLeastResumeFreeDiskSize(const int64_t& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("least-free-disk-resume-size", std::to_string(value));
    least_free_disk_to_resume_ = value;
  }
  void SetResumeInterval(const int64_t& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("manually-resume-interval", std::to_string(value));
    resume_check_interval_ = value;
  }
  void SetMinCheckResumeRatio(const double& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("min-check-resume-ratio", std::to_string(value));
    min_check_resume_ratio_ = value;
  }
  void SetSyncWindowSize(const int& value) {
    TryPushDiffCommands("sync-window-size", std::to_string(value));
    sync_window_size_.store(value);
  }
  void SetMaxConnRbufSize(const int& value) {
    TryPushDiffCommands("max-conn-rbuf-size", std::to_string(value));
    max_conn_rbuf_size_.store(value);
  }
  void SetMaxCacheFiles(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-cache-files", std::to_string(value));
    max_cache_files_ = value;
  }
  void SetMaxBackgroudCompactions(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-background-compactions", std::to_string(value));
    max_background_compactions_ = value;
  }
  void SetMaxBackgroudJobs(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-background-jobs", std::to_string(value));
    max_background_jobs_ = value;
  }
  void SetWriteBufferSize(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("write-buffer-size", std::to_string(value));
    write_buffer_size_ = value;
  }
  void SetMinWriteBufferNumberToMerge(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("min-write-buffer-number-to-merge", std::to_string(value));
    min_write_buffer_number_to_merge_ = value;
  }
  void SetLevel0StopWritesTrigger(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("level0-stop-writes-trigger", std::to_string(value));
    level0_stop_writes_trigger_ = value;
  }
  void SetLevel0SlowdownWritesTrigger(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("level0-slowdown-writes-trigger", std::to_string(value));
    level0_slowdown_writes_trigger_ = value;
  }
  void SetLevel0FileNumCompactionTrigger(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("level0-file-num-compaction-trigger", std::to_string(value));
    level0_file_num_compaction_trigger_ = value;
  }
  void SetMaxWriteBufferNumber(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-write-buffer-num", std::to_string(value));
    max_write_buffer_num_ = value;
  }
  void SetMaxTotalWalSize(uint64_t value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-total-wal-size", std::to_string(value));
    max_total_wal_size_ = value;
  }
  void SetArenaBlockSize(const int& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("arena-block-size", std::to_string(value));
    arena_block_size_ = value;
  }

  void SetLogLevel(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("loglevel", value);
    log_level_ = value;
  }

  // Rsync Rate limiting configuration
  void SetThrottleBytesPerSecond(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("throttle-bytes-per-second", std::to_string(value));
    throttle_bytes_per_second_ = value;
  }

  void SetMaxRsyncParallelNum(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("max-rsync-parallel-num", std::to_string(value));
    max_rsync_parallel_num_ = value;
  }

  void SetRsyncTimeoutMs(int64_t value){
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("rsync-timeout-ms", std::to_string(value));
    rsync_timeout_ms_.store(value);
  }

  void SetAclPubsubDefault(const std::string& value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("acl-pubsub-default", value);
    if (value == "resetchannels") {
      acl_pubsub_default_ = 0;
    } else {
      acl_pubsub_default_ = static_cast<uint32_t>(AclSelectorFlag::ALL_CHANNELS);
    }
  }
  void SetAclLogMaxLen(const int value) {
    std::lock_guard l(rwlock_);
    TryPushDiffCommands("acllog-max-len", std::to_string(value));
    acl_Log_max_len_ = value;
  }

  const std::string scache_type() {
    std::shared_lock l(rwlock_);
    return pstd::StringConcat(cache_type_, COMMA);
  }

  int64_t cache_maxmemory() { return cache_maxmemory_; }
  void SetSlowCmd(const std::string& value) {
    std::lock_guard l(rwlock_);
    std::string lower_value = value;
    pstd::StringToLower(lower_value);
    TryPushDiffCommands("slow-cmd-list", lower_value);
    pstd::StringSplit2Set(lower_value, ',', slow_cmd_set_);
  }

  void SetCacheType(const std::string &value);
  void SetCacheDisableFlag() { tmp_cache_disable_flag_ = true; }
  int zset_cache_start_direction() { return zset_cache_start_direction_; }
  int zset_cache_field_num_per_key() { return zset_cache_field_num_per_key_; }
  int cache_maxmemory_policy() { return cache_maxmemory_policy_; }
  int cache_maxmemory_samples() { return cache_maxmemory_samples_; }
  int cache_lfu_decay_time() { return cache_lfu_decay_time_; }
  int Load();
  int ConfigRewrite();
  int ConfigRewriteReplicationID();

 private:
  // TODO: replace mutex with atomic value
  int port_ = 0;
  int slave_priority_ = 0;
  int thread_num_ = 0;
  int thread_pool_size_ = 0;
  int slow_cmd_thread_pool_size_ = 0;
  std::unordered_set<std::string> slow_cmd_set_;
  int sync_thread_num_ = 0;
  int expire_dump_days_ = 3;
  int db_sync_speed_ = 0;
  std::string slaveof_;
  std::string log_path_;
  std::string log_level_;
  std::string db_path_;
  int db_instance_num_ = 0;
  std::string db_sync_path_;

  // compact
  std::string compact_cron_;
  std::string compact_interval_;
  int max_subcompactions_ = 1;
  bool disable_auto_compactions_ = false;
  int64_t resume_check_interval_ = 60; // seconds
  int64_t least_free_disk_to_resume_ = 268435456; // 256 MB
  double min_check_resume_ratio_ = 0.7;
  int64_t write_buffer_size_ = 0;
  int64_t arena_block_size_ = 0;
  int64_t slotmigrate_thread_num_ = 0;
  int64_t thread_migrate_keys_num_ = 0;
  int64_t max_write_buffer_size_ = 0;
  int64_t max_total_wal_size_ = 0;
  int max_write_buffer_num_ = 0;
  int min_write_buffer_number_to_merge_ = 1;
  int level0_stop_writes_trigger_ =  36;
  int level0_slowdown_writes_trigger_ = 20;
  int level0_file_num_compaction_trigger_ = 4;
  int64_t max_client_response_size_ = 0;
  bool daemonize_ = false;
  int timeout_ = 0;
  std::string server_id_;
  std::string run_id_;
  std::string replication_id_;
  std::string requirepass_;
  std::string masterauth_;
  std::string userpass_;
  std::vector<std::string> user_blacklist_;
  std::atomic<bool> classic_mode_;
  int databases_ = 0;
  int default_slot_num_ = 1;
  std::vector<DBStruct> db_structs_;
  std::string default_db_;
  std::string bgsave_path_;
  std::string bgsave_prefix_;
  std::string pidfile_;

  std::string compression_;
  std::string compression_per_level_;
  int maxclients_ = 0;
  int root_connection_num_ = 0;
  std::atomic<bool> slowlog_write_errorlog_;
  std::atomic<int> slowlog_log_slower_than_;
  std::atomic<bool> slotmigrate_;
  std::atomic<int> binlog_writer_num_;
  int slowlog_max_len_ = 0;
  int expire_logs_days_ = 0;
  int expire_logs_nums_ = 0;
  bool slave_read_only_ = false;
  std::string conf_path_;

  int max_cache_statistic_keys_ = 0;
  int small_compaction_threshold_ = 0;
  int small_compaction_duration_threshold_ = 0;
  int max_background_flushes_ = 1;
  int max_background_compactions_ = 2;
  int max_background_jobs_ = 0;
  int max_cache_files_ = 0;
  std::atomic<uint64_t> rocksdb_ttl_second_ = 0;
  std::atomic<uint64_t> rocksdb_periodic_second_ = 0;
  int max_bytes_for_level_multiplier_ = 0;
  int64_t block_size_ = 0;
  int64_t block_cache_ = 0;
  int64_t num_shard_bits_ = 0;
  bool share_block_cache_ = false;
  bool enable_partitioned_index_filters_ = false;
  bool cache_index_and_filter_blocks_ = false;
  bool pin_l0_filter_and_index_blocks_in_cache_ = false;
  bool optimize_filters_for_hits_ = false;
  bool level_compaction_dynamic_level_bytes_ = true;
  int rate_limiter_mode_ = 0;                              // kReadsOnly = 0, kWritesOnly = 1, kAllIo = 2
  int64_t rate_limiter_bandwidth_ = 0;
  int64_t rate_limiter_refill_period_us_ = 0;
  int64_t rate_limiter_fairness_ = 0;
  bool rate_limiter_auto_tuned_ = true;

  std::atomic<int> sync_window_size_;
  std::atomic<int> max_conn_rbuf_size_;
  std::atomic<int> consensus_level_;
  std::atomic<int> replication_num_;

  std::string network_interface_;

  std::string userblacklist_;
  std::vector<std::string> users_;  // acl user rules

  std::string aclFile_;
  std::vector<std::string> cmds_;
  std::atomic<uint32_t> acl_pubsub_default_ = 0;  // default channel pub/sub permission
  std::atomic<uint32_t> acl_Log_max_len_ = 0;      // default acl log max len

  // diff commands between cached commands and config file commands
  std::map<std::string, std::string> diff_commands_;
  void TryPushDiffCommands(const std::string& command, const std::string& value);

  //
  // Critical configure items
  //
  bool write_binlog_ = false;
  int target_file_size_base_ = 0;
  int binlog_file_size_ = 0;

  // cache
  std::vector<std::string> cache_type_;
  std::atomic_bool tmp_cache_disable_flag_ = false;
  std::atomic_int64_t cache_maxmemory_ = 10737418240;
  std::atomic_int cache_num_ = 5;
  std::atomic_int cache_mode_ = 1;
  std::atomic_int cache_string_ = 1;
  std::atomic_int cache_set_ = 1;
  std::atomic_int cache_zset_ = 1;
  std::atomic_int cache_hash_ = 1;
  std::atomic_int cache_list_ = 1;
  std::atomic_int cache_bit_ = 1;
  std::atomic_int zset_cache_start_direction_ = 0;
  std::atomic_int zset_cache_field_num_per_key_ = 512;
  std::atomic_int cache_maxmemory_policy_ = 1;
  std::atomic_int cache_maxmemory_samples_ = 5;
  std::atomic_int cache_lfu_decay_time_ = 1;

  // rocksdb blob
  bool enable_blob_files_ = false;
  bool enable_blob_garbage_collection_ = false;
  double blob_garbage_collection_age_cutoff_ = 0.25;
  double blob_garbage_collection_force_threshold_ = 1.0;
  int64_t min_blob_size_ = 4096;                // 4K
  int64_t blob_cache_ = 0;
  int64_t blob_num_shard_bits_ = 0;
  int64_t blob_file_size_ = 256 * 1024 * 1024;  // 256M
  std::string blob_compression_type_ = "none";

  std::shared_mutex rwlock_;

  // Rsync Rate limiting configuration
  int throttle_bytes_per_second_ = 207200000;
  int max_rsync_parallel_num_ = kMaxRsyncParallelNum;
  std::atomic_int64_t rsync_timeout_ms_ = 1000;
};

#endif
