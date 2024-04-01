// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <shared_mutex>
#if defined(__APPLE__)
#  include <sys/mount.h>
#  include <sys/param.h>
#else
#  include <sys/statfs.h>
#endif
#include <memory>
#include <set>

#include "src/cache/include/config.h"
#include "net/include/bg_thread.h"
#include "net/include/net_pubsub.h"
#include "net/include/thread_pool.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "storage/backupable.h"
#include "storage/storage.h"

#include "acl.h"
#include "include/pika_auxiliary_thread.h"
#include "include/pika_binlog.h"
#include "include/pika_cache.h"
#include "include/pika_client_processor.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_db.h"
#include "include/pika_define.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_instant.h"
#include "include/pika_migrate_thread.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_rsync_service.h"
#include "include/pika_slot_command.h"
#include "include/pika_statistic.h"
#include "include/pika_transaction.h"
#include "include/rsync_server.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

enum TaskType {
  kCompactAll,
  kCompactStrings,
  kCompactHashes,
  kCompactSets,
  kCompactZSets,
  kCompactList,
  kResetReplState,
  kPurgeLog,
  kStartKeyScan,
  kStopKeyScan,
  kBgSave,
  kCompactRangeStrings,
  kCompactRangeHashes,
  kCompactRangeSets,
  kCompactRangeZSets,
  kCompactRangeList,
};

struct TaskArg {
  TaskType type;
  std::vector<std::string> argv;
  TaskArg(TaskType t) : type(t) {}
  TaskArg(TaskType t, const std::vector<std::string>& a) : type(t), argv(a) {}
};

void DoBgslotscleanup(void* arg);
void DoBgslotsreload(void* arg);

class PikaServer : public pstd::noncopyable {
 public:
  PikaServer();
  ~PikaServer();

  /*
   * Server init info
   */
  bool ServerInit();
  void Start();
  void Exit();

  std::string host();
  int port();
  time_t start_time_s();
  std::string master_ip();
  int master_port();
  int role();
  bool leader_protected_mode();
  void CheckLeaderProtectedMode();
  bool readonly(const std::string& table);
  int repl_state();
  std::string repl_state_str();
  bool force_full_sync();
  void SetForceFullSync(bool v);
  void SetDispatchQueueLimit(int queue_limit);
  storage::StorageOptions storage_options();
  std::unique_ptr<PikaDispatchThread>& pika_dispatch_thread() {
    return pika_dispatch_thread_;
  }

  /*
   * DB use
   */
  void InitDBStruct();
  bool IsBgSaving();
  bool IsKeyScaning();
  bool IsCompacting();
  bool IsDBExist(const std::string& db_name);
  bool IsDBBinlogIoError(const std::string& db_name);
  std::shared_ptr<DB> GetDB(const std::string& db_name);
  std::set<std::string> GetAllDBName();
  pstd::Status DoSameThingSpecificDB(const std::set<std::string>& dbs, const TaskArg& arg);
  std::shared_mutex& GetDBLock() {
    return dbs_rw_;
  }
  void DBLockShared() {
    dbs_rw_.lock_shared();
  }
  void DBLock() {
    dbs_rw_.lock();
  }
  void DBUnlock() {
    dbs_rw_.unlock();
  }
  void DBUnlockShared() {
    dbs_rw_.unlock_shared();
  }

  /*
   * DB use
   */
  void PrepareDBTrySync();
  void DBSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys);
  void DBSetSmallCompactionThreshold(uint32_t small_compaction_threshold);
  void DBSetSmallCompactionDurationThreshold(uint32_t small_compaction_duration_threshold);
  bool GetDBBinlogOffset(const std::string& db_name, BinlogOffset* boffset);
  pstd::Status DoSameThingEveryDB(const TaskType& type);

  /*
   * Master use
   */
  void BecomeMaster();
  void DeleteSlave(int fd);  // conn fd
  int32_t CountSyncSlaves();
  int32_t GetSlaveListString(std::string& slave_list_str);
  bool TryAddSlave(const std::string& ip, int64_t port, int fd, const std::vector<DBStruct>& table_structs);
  pstd::Mutex slave_mutex_;  // protect slaves_;
  std::vector<SlaveItem> slaves_;

  /**
   * Sotsmgrt use
   */
  std::unique_ptr<PikaMigrate> pika_migrate_;

  /*
   * Slave use
   */
  void SyncError();
  void RemoveMaster();
  bool SetMaster(std::string& master_ip, int master_port);

  /*
   * Slave State Machine
   */
  bool ShouldMetaSync();
  void FinishMetaSync();
  bool MetaSyncDone();
  void ResetMetaSyncStatus();
  void SetLoopDBStateMachine(bool need_loop);
  int GetMetaSyncTimestamp();
  void UpdateMetaSyncTimestamp();
  void UpdateMetaSyncTimestampWithoutLock();
  bool IsFirstMetaSync();
  void SetFirstMetaSync(bool v);

  /*
   * PikaClientProcessor Process Task
   */
  void ScheduleClientPool(net::TaskFunc func, void* arg, bool is_slow_cmd);
  void ScheduleClientBgThreads(net::TaskFunc func, void* arg, const std::string& hash_str);
  // for info debug
  size_t ClientProcessorThreadPoolCurQueueSize();
  size_t ClientProcessorThreadPoolMaxQueueSize();
  size_t SlowCmdThreadPoolCurQueueSize();
  size_t SlowCmdThreadPoolMaxQueueSize();

  /*
   * BGSave used
   */
  void BGSaveTaskSchedule(net::TaskFunc func, void* arg);

  /*
   * PurgeLog used
   */
  void PurgelogsTaskSchedule(net::TaskFunc func, void* arg);

  /*
   * Flushall & Flushdb used
   */
  void PurgeDir(const std::string& path);
  void PurgeDirTaskSchedule(void (*function)(void*), void* arg);

  /*
   * DBSync used
   */
  pstd::Status GetDumpUUID(const std::string& db_name, std::string* snapshot_uuid);
  pstd::Status GetDumpMeta(const std::string& db_name, std::vector<std::string>* files, std::string* snapshot_uuid);
  void TryDBSync(const std::string& ip, int port, const std::string& db_name, int32_t top);

  /*
   * Keyscan used
   */
  void KeyScanTaskSchedule(net::TaskFunc func, void* arg);

  /*
   * Client used
   */
  void ClientKillAll();
  int ClientKill(const std::string& ip_port);
  int64_t ClientList(std::vector<ClientInfo>* clients = nullptr);

  /*
   * Monitor used
   */
  bool HasMonitorClients() const;
  bool ClientIsMonitor(const std::shared_ptr<PikaClientConn>& client_ptr) const;
  void AddMonitorMessage(const std::string& monitor_message);
  void AddMonitorClient(const std::shared_ptr<PikaClientConn>& client_ptr);

  /*
   * Slowlog used
   */
  void SlowlogTrim();
  void SlowlogReset();
  void SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs);
  void SlowlogPushEntry(const std::vector<std::string>& argv, int64_t time, int64_t duration);
  uint32_t SlowlogLen();
  uint64_t SlowlogCount();

  /*
   * Statistic used
   */
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  uint64_t accumulative_connections();
  void ResetStat();
  void incr_accumulative_connections();
  void ResetLastSecQuerynum();
  void UpdateQueryNumAndExecCountDB(const std::string& db_name, const std::string& command, bool is_write);
  std::unordered_map<std::string, uint64_t> ServerExecCountDB();
  std::unordered_map<std::string, QpsStatistic> ServerAllDBStat();

  /*
   * Disk usage statistic
   */
  uint64_t GetDBSize() const {
    return disk_statistic_.db_size_.load();
  }
  uint64_t GetLogSize() const {
    return disk_statistic_.log_size_.load();
  }

  /*
   * Network Statistic used
   */
  size_t NetInputBytes();
  size_t NetOutputBytes();
  size_t NetReplInputBytes();
  size_t NetReplOutputBytes();
  float InstantaneousInputKbps();
  float InstantaneousOutputKbps();
  float InstantaneousInputReplKbps();
  float InstantaneousOutputReplKbps();

  /*
   * Slave to Master communication used
   */
  int SendToPeer();
  void SignalAuxiliary();
  pstd::Status TriggerSendBinlogSync();

  /*
   * PubSub used
   */
  int PubSubNumPat();
  int Publish(const std::string& channel, const std::string& msg);
  void EnablePublish(int fd);
  int UnSubscribe(const std::shared_ptr<net::NetConn>& conn, const std::vector<std::string>& channels, bool pattern,
                  std::vector<std::pair<std::string, int>>* result);
  void Subscribe(const std::shared_ptr<net::NetConn>& conn, const std::vector<std::string>& channels, bool pattern,
                 std::vector<std::pair<std::string, int>>* result);
  void PubSubChannels(const std::string& pattern, std::vector<std::string>* result);
  void PubSubNumSub(const std::vector<std::string>& channels, std::vector<std::pair<std::string, int>>* result);
  int ClientPubSubChannelSize(const std::shared_ptr<net::NetConn>& conn);
  int ClientPubSubChannelPatternSize(const std::shared_ptr<net::NetConn>& conn);

  pstd::Status GetCmdRouting(std::vector<net::RedisCmdArgsType>& redis_cmds, std::vector<Node>* dst, bool* all_local);

  // info debug use
  void ServerStatus(std::string* info);

  /*
   * Async migrate used
   */
  int SlotsMigrateOne(const std::string& key, const std::shared_ptr<DB> &db);
  bool SlotsMigrateBatch(const std::string &ip, int64_t port, int64_t time_out, int64_t slots, int64_t keys_num, const std::shared_ptr<DB>& db);
  void GetSlotsMgrtSenderStatus(std::string *ip, int64_t* port, int64_t *slot, bool *migrating, int64_t *moved, int64_t *remained);
  bool SlotsMigrateAsyncCancel();
  std::shared_mutex bgsave_protector_;
  BgSaveInfo bgsave_info_;

  /*
   * BGSlotsReload used
   */
  struct BGSlotsReload {
    bool reloading = false;
    time_t start_time = 0;
    time_t end_time = 0;
    std::string s_start_time;
    int64_t cursor = 0;
    std::string pattern = "*";
    int64_t count = 100;
    std::shared_ptr<DB> db;
    BGSlotsReload() = default;
    void Clear() {
      reloading = false;
      pattern = "*";
      count = 100;
      cursor = 0;
    }
  };

  BGSlotsReload bgslots_reload_;

  BGSlotsReload bgslots_reload() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_reload_;
  }
  bool GetSlotsreloading() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_reload_.reloading;
  }
  void SetSlotsreloading(bool reloading) {
    std::lock_guard ml(bgsave_protector_);
    bgslots_reload_.reloading = reloading;
  }
  void SetSlotsreloadingCursor(int64_t cursor) {
    std::lock_guard ml(bgsave_protector_);
    bgslots_reload_.cursor = cursor;
  }
  int64_t GetSlotsreloadingCursor() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_reload_.cursor;
  }

  void SetSlotsreloadingEndTime() {
    std::lock_guard ml(bgsave_protector_);
    bgslots_reload_.end_time = time(nullptr);
  }
  void Bgslotsreload(const std::shared_ptr<DB>& db);

  // Revoke the authorization of the specified account, when handle Cmd deleteUser
  void AllClientUnAuth(const std::set<std::string>& users);

  // Determine whether the user's conn can continue to subscribe to the channel
  void CheckPubsubClientKill(const std::string& userName, const std::vector<std::string>& allChannel);

  /*
   * BGSlotsCleanup used
   */
  struct BGSlotsCleanup {
    bool cleaningup = false;
    time_t start_time = 0;
    time_t end_time = 0;
    std::string s_start_time;
    int64_t cursor = 0;
    std::string pattern = "*";
    int64_t count = 100;
    std::shared_ptr<DB> db;
    storage::DataType type_;
    std::vector<int> cleanup_slots;
    BGSlotsCleanup() = default;
    void Clear() {
      cleaningup = false;
      pattern = "*";
      count = 100;
      cursor = 0;
    }
  };

  /*
   * BGSlotsCleanup use
   */
  BGSlotsCleanup bgslots_cleanup_;
  net::BGThread bgslots_cleanup_thread_;

  BGSlotsCleanup bgslots_cleanup() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_cleanup_;
  }
  bool GetSlotscleaningup() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_cleanup_.cleaningup;
  }
  void SetSlotscleaningup(bool cleaningup) {
    std::lock_guard ml(bgsave_protector_);
    bgslots_cleanup_.cleaningup = cleaningup;
  }
  void SetSlotscleaningupCursor(int64_t cursor) {
    std::lock_guard ml(bgsave_protector_);
    bgslots_cleanup_.cursor = cursor;
  }
  void SetCleanupSlots(std::vector<int> cleanup_slots) {
    std::lock_guard ml(bgsave_protector_);
    bgslots_cleanup_.cleanup_slots.swap(cleanup_slots);
  }
  std::vector<int> GetCleanupSlots() {
    std::lock_guard ml(bgsave_protector_);
    return bgslots_cleanup_.cleanup_slots;
  }

  void Bgslotscleanup(std::vector<int> cleanup_slots, const std::shared_ptr<DB>& db);
  void StopBgslotscleanup() {
    std::lock_guard ml(bgsave_protector_);
    bgslots_cleanup_.cleaningup = false;
    std::vector<int> cleanup_slots;
    bgslots_cleanup_.cleanup_slots.swap(cleanup_slots);
  }

  /*
   * StorageOptions used
   */
  storage::Status RewriteStorageOptions(const storage::OptionType& option_type,
                                        const std::unordered_map<std::string, std::string>& options);

 /*
  * Instantaneous Metric used
  */
  std::unique_ptr<Instant> instant_;

 /*
  * Diskrecovery used
  */
  std::map<std::string, std::shared_ptr<DB>> GetDB() {
    return dbs_;
  }

  /*
   * acl init
   */
  pstd::Status InitAcl() { return acl_->Initialization(); }

  std::unique_ptr<::Acl>& Acl() { return acl_; }

  friend class Cmd;
  friend class InfoCmd;
  friend class PikaReplClientConn;
  friend class PkClusterInfoCmd;

  struct BGCacheTaskArg {
    BGCacheTaskArg() : conf(nullptr), reenable_cache(false) {}
    int task_type;
    std::shared_ptr<DB> db;
    uint32_t cache_num;
    cache::CacheConfig cache_cfg;
    std::unique_ptr<PikaConf> conf;
    bool reenable_cache;
  };

  /*
   * Cache used
   */
  static void DoCacheBGTask(void* arg);
  void ResetCacheAsync(uint32_t cache_num, std::shared_ptr<DB> db, cache::CacheConfig *cache_cfg = nullptr);
  void ClearCacheDbAsync(std::shared_ptr<DB> db);
  void ClearCacheDbAsyncV2(std::shared_ptr<DB> db);
  void ResetCacheConfig(std::shared_ptr<DB> db);
  void ClearHitRatio(std::shared_ptr<DB> db);
  void OnCacheStartPosChanged(int zset_cache_start_direction, std::shared_ptr<DB> db);
  void UpdateCacheInfo(void);
  void ResetDisplayCacheInfo(int status, std::shared_ptr<DB> db);
  void CacheConfigInit(cache::CacheConfig &cache_cfg);
  void ProcessCronTask();
  double HitRatio();

  /*
  * disable compact
  */
  void DisableCompact();

  /*
   * lastsave used
   */
  int64_t GetLastSave() const {return lastsave_;}
  void UpdateLastSave(int64_t lastsave) {lastsave_ = lastsave;}
 private:
  /*
   * TimingTask use
   */
  void DoTimingTask();
  void AutoCompactRange();
  void AutoPurge();
  void AutoDeleteExpiredDump();
  void AutoUpdateNetworkMetric();
  void PrintThreadPoolQueueStatus();
  void StatDiskUsage();
  int64_t GetLastSaveTime(const std::string& dump_dir);

  std::string host_;
  int port_ = 0;
  time_t start_time_s_ = 0;

  std::shared_mutex storage_options_rw_;
  storage::StorageOptions storage_options_;
  void InitStorageOptions();

  std::atomic<bool> exit_;
  std::timed_mutex exit_mutex_;

  /*
   * DB used
   */
  std::shared_mutex dbs_rw_;
  std::map<std::string, std::shared_ptr<DB>> dbs_;

  /*
   * CronTask used
   */
  bool have_scheduled_crontask_ = false;
  struct timeval last_check_compact_time_;

  /*
   * ResumeDB used
   */
  struct timeval last_check_resume_time_;

  /*
   * Communicate with the client used
   */
  int worker_num_ = 0;
  std::unique_ptr<PikaClientProcessor> pika_client_processor_;
  std::unique_ptr<net::ThreadPool> pika_slow_cmd_thread_pool_;
  std::unique_ptr<PikaDispatchThread> pika_dispatch_thread_ = nullptr;

  /*
   * Slave used
   */
  std::string master_ip_;
  int master_port_ = 0;
  int repl_state_ = PIKA_REPL_NO_CONNECT;
  int role_ = PIKA_ROLE_SINGLE;
  int last_meta_sync_timestamp_ = 0;
  bool first_meta_sync_ = false;
  bool force_full_sync_ = false;
  bool leader_protected_mode_ = false;  // reject request after master slave sync done
  std::shared_mutex state_protector_;   // protect below, use for master-slave mode

  /*
   * Bgsave used
   */
  net::BGThread bgsave_thread_;

  /*
   * Purgelogs use
   */
  net::BGThread purge_thread_;

  /*
   * Keyscan used
   */
  net::BGThread key_scan_thread_;

  /*
   * Monitor used
   */
  mutable pstd::Mutex monitor_mutex_protector_;
  std::set<std::weak_ptr<PikaClientConn>, std::owner_less<std::weak_ptr<PikaClientConn>>> pika_monitor_clients_;

  /*
   * Rsync used
   */
  std::unique_ptr<PikaRsyncService> pika_rsync_service_;
  std::unique_ptr<rsync::RsyncServer> rsync_server_;

  /*
   * Pubsub used
   */
  std::unique_ptr<net::PubSubThread> pika_pubsub_thread_;

  /*
   * Communication used
   */
  std::unique_ptr<PikaAuxiliaryThread> pika_auxiliary_thread_;

  /*
   * Async slotsMgrt use
   */
  std::unique_ptr<PikaMigrateThread> pika_migrate_thread_;

  /*
   * Slowlog used
   */
  uint64_t slowlog_entry_id_ = 0;
  uint64_t slowlog_counter_ = 0;
  std::shared_mutex slowlog_protector_;
  std::list<SlowlogEntry> slowlog_list_;

  /*
   * Statistic used
   */
  Statistic statistic_;

  DiskStatistic disk_statistic_;

  net::BGThread common_bg_thread_;

  /*
   * Cache used
   */
  std::shared_mutex mu_;
  std::shared_mutex cache_info_rwlock_;

  /*
   * lastsave used
   */
  int64_t lastsave_ = 0;

  /*
   * acl
   */
  std::unique_ptr<::Acl> acl_ = nullptr;
};

#endif
