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

#include "net/include/bg_thread.h"
#include "net/include/net_pubsub.h"
#include "net/include/thread_pool.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "pstd/include/pstd_string.h"
#include "storage/backupable.h"
#include "storage/storage.h"

#include "include/pika_auxiliary_thread.h"
#include "include/pika_binlog.h"
#include "include/pika_client_processor.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_rsync_service.h"
#include "include/pika_statistic.h"
#include "include/pika_table.h"

using pstd::Slice;
using pstd::Status;

/*
static std::set<std::string> MultiKvCommands {kCmdNameDel,
             kCmdNameMget,        kCmdNameKeys,              kCmdNameMset,
             kCmdNameMsetnx,      kCmdNameExists,            kCmdNameScan,
             kCmdNameScanx,       kCmdNamePKScanRange,       kCmdNamePKRScanRange,
             kCmdNameRPopLPush,   kCmdNameZUnionstore,       kCmdNameZInterstore,
             kCmdNameSUnion,      kCmdNameSUnionstore,       kCmdNameSInter,
             kCmdNameSInterstore, kCmdNameSDiff,             kCmdNameSDiffstore,
             kCmdNameSMove,       kCmdNameBitOp,             kCmdNamePfAdd,
             kCmdNamePfCount,     kCmdNamePfMerge,           kCmdNameGeoAdd,
             kCmdNameGeoPos,      kCmdNameGeoDist,           kCmdNameGeoHash,
             kCmdNameGeoRadius,   kCmdNameGeoRadiusByMember};
*/

static std::set<std::string> ConsensusNotSupportCommands{kCmdNameMsetnx,
                                                         kCmdNameScan,
                                                         kCmdNameKeys,
                                                         kCmdNameRPopLPush,
                                                         kCmdNameZUnionstore,
                                                         kCmdNameZInterstore,
                                                         kCmdNameSUnion,
                                                         kCmdNameSUnionstore,
                                                         kCmdNameSInter,
                                                         kCmdNameSInterstore,
                                                         kCmdNameSDiff,
                                                         kCmdNameSDiffstore,
                                                         kCmdNameSMove,
                                                         kCmdNameBitOp,
                                                         kCmdNamePfAdd,
                                                         kCmdNamePfCount,
                                                         kCmdNamePfMerge,
                                                         kCmdNameGeoAdd,
                                                         kCmdNameGeoPos,
                                                         kCmdNameGeoDist,
                                                         kCmdNameGeoHash,
                                                         kCmdNameGeoRadius,
                                                         kCmdNameGeoRadiusByMember,
                                                         kCmdNamePKPatternMatchDel,
                                                         kCmdNameSlaveof,
                                                         kCmdNameDbSlaveof,
                                                         kCmdNameMset,
                                                         kCmdNameMget,
                                                         kCmdNameScanx};

static std::set<std::string> ShardingModeNotSupportCommands{kCmdNameMsetnx,
                                                            kCmdNameScan,
                                                            kCmdNameKeys,
                                                            kCmdNameScanx,
                                                            kCmdNameZUnionstore,
                                                            kCmdNameZInterstore,
                                                            kCmdNameSUnion,
                                                            kCmdNameSUnionstore,
                                                            kCmdNameSInter,
                                                            kCmdNameSInterstore,
                                                            kCmdNameSDiff,
                                                            kCmdNameSDiffstore,
                                                            kCmdNameSMove,
                                                            kCmdNameBitOp,
                                                            kCmdNamePfAdd,
                                                            kCmdNamePfCount,
                                                            kCmdNamePfMerge,
                                                            kCmdNameGeoAdd,
                                                            kCmdNameGeoPos,
                                                            kCmdNameGeoDist,
                                                            kCmdNameGeoHash,
                                                            kCmdNameGeoRadius,
                                                            kCmdNameGeoRadiusByMember,
                                                            kCmdNamePKPatternMatchDel,
                                                            kCmdNameSlaveof,
                                                            kCmdNameDbSlaveof};

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
};

class PikaServer {
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
  bool readonly(const std::string& table, const std::string& key);
  bool ConsensusCheck(const std::string& table_name, const std::string& key);
  int repl_state();
  std::string repl_state_str();
  bool force_full_sync();
  void SetForceFullSync(bool v);
  void SetDispatchQueueLimit(int queue_limit);
  storage::StorageOptions storage_options();

  /*
   * Table use
   */
  void InitTableStruct();
  Status AddTableStruct(std::string table_name, uint32_t num);
  Status DelTableStruct(std::string table_name);
  std::shared_ptr<Table> GetTable(const std::string& table_name);
  std::set<uint32_t> GetTablePartitionIds(const std::string& table_name);
  bool IsBgSaving();
  bool IsKeyScaning();
  bool IsCompacting();
  bool IsTableExist(const std::string& table_name);
  bool IsTablePartitionExist(const std::string& table_name, uint32_t partition_id);
  bool IsCommandSupport(const std::string& command);
  bool IsTableBinlogIoError(const std::string& table_name);
  Status DoSameThingSpecificTable(const TaskType& type, const std::set<std::string>& tables = {});

  /*
   * Partition use
   */
  void PreparePartitionTrySync();
  void PartitionSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys);
  void PartitionSetSmallCompactionThreshold(uint32_t small_compaction_threshold);
  bool GetTablePartitionBinlogOffset(const std::string& table_name, uint32_t partition_id, BinlogOffset* const boffset);
  std::shared_ptr<Partition> GetPartitionByDbName(const std::string& db_name);
  std::shared_ptr<Partition> GetTablePartitionById(const std::string& table_name, uint32_t partition_id);
  std::shared_ptr<Partition> GetTablePartitionByKey(const std::string& table_name, const std::string& key);
  Status DoSameThingEveryPartition(const TaskType& type);

  /*
   * Master use
   */
  void BecomeMaster();
  void DeleteSlave(int fd);  // conn fd
  int32_t CountSyncSlaves();
  int32_t GetSlaveListString(std::string& slave_list_str);
  int32_t GetShardingSlaveListString(std::string& slave_list_str);
  bool TryAddSlave(const std::string& ip, int64_t port, int fd, const std::vector<TableStruct>& table_structs);
  pstd::Mutex slave_mutex_;  // protect slaves_;
  std::vector<SlaveItem> slaves_;

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
  bool AllPartitionConnectSuccess();
  bool LoopPartitionStateMachine();
  void SetLoopPartitionStateMachine(bool need_loop);
  int GetMetaSyncTimestamp();
  void UpdateMetaSyncTimestamp();
  bool IsFirstMetaSync();
  void SetFirstMetaSync(bool v);

  /*
   * PikaClientProcessor Process Task
   */
  void ScheduleClientPool(net::TaskFunc func, void* arg);
  void ScheduleClientBgThreads(net::TaskFunc func, void* arg, const std::string& hash_str);
  // for info debug
  size_t ClientProcessorThreadPoolCurQueueSize();

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
  void DBSync(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id);
  void TryDBSync(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id, int32_t top);
  void DbSyncSendFile(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id);
  std::string DbSyncTaskIndex(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id);

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
  void AddMonitorMessage(const std::string& monitor_message);
  void AddMonitorClient(std::shared_ptr<PikaClientConn> client_ptr);

  /*
   * Slowlog used
   */
  void SlowlogTrim();
  void SlowlogReset();
  uint32_t SlowlogLen();
  void SlowlogObtain(int64_t number, std::vector<SlowlogEntry>* slowlogs);
  void SlowlogPushEntry(const PikaCmdArgsType& argv, int32_t time, int64_t duration);

  /*
   * Statistic used
   */
  void ResetStat();
  uint64_t ServerQueryNum();
  uint64_t ServerCurrentQps();
  uint64_t accumulative_connections();
  void incr_accumulative_connections();
  void ResetLastSecQuerynum();
  void UpdateQueryNumAndExecCountTable(const std::string& table_name, const std::string& command, bool is_write);
  std::unordered_map<std::string, uint64_t> ServerExecCountTable();
  QpsStatistic ServerTableStat(const std::string& table_name);
  std::unordered_map<std::string, QpsStatistic> ServerAllTableStat();
  /*
   * Slave to Master communication used
   */
  int SendToPeer();
  void SignalAuxiliary();
  Status TriggerSendBinlogSync();

  /*
   * PubSub used
   */
  int PubSubNumPat();
  int Publish(const std::string& channel, const std::string& msg);
  void EnablePublish(int fd);
  int UnSubscribe(std::shared_ptr<net::NetConn> conn, const std::vector<std::string>& channels, const bool pattern,
                  std::vector<std::pair<std::string, int>>* result);
  void Subscribe(std::shared_ptr<net::NetConn> conn, const std::vector<std::string>& channels, const bool pattern,
                 std::vector<std::pair<std::string, int>>* result);
  void PubSubChannels(const std::string& pattern, std::vector<std::string>* result);
  void PubSubNumSub(const std::vector<std::string>& channels, std::vector<std::pair<std::string, int>>* result);

  Status GetCmdRouting(std::vector<net::RedisCmdArgsType>& redis_cmds, std::vector<Node>* dst, bool* all_local);

  // info debug use
  void ServerStatus(std::string* info);

  /*
   * StorageOptions used
   */
  storage::Status RewriteStorageOptions(const storage::OptionType& option_type,
                                        const std::unordered_map<std::string, std::string>& options);

  friend class Cmd;
  friend class InfoCmd;
  friend class PkClusterAddSlotsCmd;
  friend class PkClusterDelSlotsCmd;
  friend class PkClusterAddTableCmd;
  friend class PkClusterDelTableCmd;
  friend class PikaReplClientConn;
  friend class PkClusterInfoCmd;

 private:
  /*
   * TimingTask use
   */
  void DoTimingTask();
  void AutoCompactRange();
  void AutoPurge();
  void AutoDeleteExpiredDump();
  void AutoKeepAliveRSync();

  std::string host_;
  int port_ = 0;
  time_t start_time_s_ = 0;

  std::shared_mutex storage_options_rw_;
  storage::StorageOptions storage_options_;
  void InitStorageOptions();

  std::atomic<bool> exit_;

  /*
   * Table used
   */
  std::atomic<SlotState> slot_state_;
  std::shared_mutex tables_rw_;
  std::map<std::string, std::shared_ptr<Table>> tables_;

  /*
   * CronTask used
   */
  bool have_scheduled_crontask_ = false;
  struct timeval last_check_compact_time_;

  /*
   * Communicate with the client used
   */
  int worker_num_ = 0;
  std::unique_ptr<PikaClientProcessor> pika_client_processor_;
  std::unique_ptr<PikaDispatchThread> pika_dispatch_thread_ = nullptr;

  /*
   * Slave used
   */
  std::string master_ip_;
  int master_port_  = 0;
  int repl_state_  = PIKA_REPL_NO_CONNECT;
  int role_ = PIKA_ROLE_SINGLE;
  int last_meta_sync_timestamp_ = 0;
  bool first_meta_sync_ = false;
  bool loop_partition_state_machine_ = false;
  bool force_full_sync_ = false;
  bool leader_protected_mode_ = false;        // reject request after master slave sync done
  std::shared_mutex state_protector_;         // protect below, use for master-slave mode

  /*
   * Bgsave used
   */
  net::BGThread bgsave_thread_;

  /*
   * Purgelogs use
   */
  net::BGThread purge_thread_;

  /*
   * DBSync used
   */
  pstd::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;

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

  /*
   * Pubsub used
   */
  std::unique_ptr<net::PubSubThread> pika_pubsub_thread_;

  /*
   * Communication used
   */
  std::unique_ptr<PikaAuxiliaryThread> pika_auxiliary_thread_;

  /*
   * Slowlog used
   */
  uint64_t slowlog_entry_id_ = 0;
  std::shared_mutex slowlog_protector_;
  std::list<SlowlogEntry> slowlog_list_;

  /*
   * Statistic used
   */
  Statistic statistic_;

  PikaServer(PikaServer& ps);
  void operator=(const PikaServer& ps);
};

#endif
