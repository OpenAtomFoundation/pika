// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include <sys/statfs.h>
#include <memory>

#include "slash/include/slash_mutex.h"
#include "slash/include/slash_status.h"
#include "pink/include/bg_thread.h"
#include "pink/include/thread_pool.h"
#include "pink/include/pink_pubsub.h"
#include "blackwidow/blackwidow.h"
#include "blackwidow/backupable.h"

#include "include/pika_conf.h"
#include "include/pika_table.h"
#include "include/pika_binlog.h"
#include "include/pika_define.h"
#include "include/pika_monitor_thread.h"
#include "include/pika_rsync_service.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_auxiliary_thread.h"

using slash::Status;
using slash::Slice;

struct StatisticData {
  StatisticData()
      : accumulative_connections(0),
        thread_querynum(0),
        last_thread_querynum(0),
        last_sec_thread_querynum(0),
        last_time_us(0) {
  }

  slash::RWMutex statistic_lock;
  std::atomic<uint64_t> accumulative_connections;
  std::unordered_map<std::string, uint64_t> exec_count_table;
  uint64_t thread_querynum;
  uint64_t last_thread_querynum;
  uint64_t last_sec_thread_querynum;
  uint64_t last_time_us;
};

static std::set<std::string> ShardingModeNotSupportCommands {kCmdNameDel,
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

extern PikaConf *g_pika_conf;

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
  int64_t sid();
  void SetSid(int64_t sid);
  int role();
  bool readonly();
  int repl_state();
  std::string repl_state_str();
  bool force_full_sync();
  void SetForceFullSync(bool v);
  void SetDispatchQueueLimit(int queue_limit);


  /*
   * Table use
   */
  void InitTableStruct();
  bool RebuildTableStruct(const std::vector<TableStruct>& table_structs);
  std::shared_ptr<Table> GetTable(const std::string& table_name);
  bool IsBgSaving();
  bool IsKeyScaning();
  bool IsCompacting();
  bool IsTableExist(const std::string& table_name);
  bool IsCommandSupport(const std::string& command);
  bool IsTableBinlogIoError(const std::string& table_name);
  Status DoSameThingEveryTable(const TaskType& type);

  /*
   * Partition use
   */
  void PreparePartitionTrySync();
  void PartitionSetMaxCacheStatisticKeys(uint32_t max_cache_statistic_keys);
  void PartitionSetSmallCompactionThreshold(uint32_t small_compaction_threshold);
  bool PartitionCouldPurge(const std::string& table_name,
                           uint32_t partition_id, uint32_t index);
  bool GetTablePartitionBinlogOffset(const std::string& table_name,
                                     uint32_t partition_id,
                                     BinlogOffset* const boffset);
  std::shared_ptr<Partition> GetTablePartitionById(
                                  const std::string& table_name,
                                  uint32_t partition_id);
  std::shared_ptr<Partition> GetTablePartitionByKey(
                                  const std::string& table_name,
                                  const std::string& key);
  Status DoSameThingEveryPartition(const TaskType& type);

  /*
   * Master use
   */
  int64_t GenSid();
  void BecomeMaster();
  void DeleteSlave(int fd);   //conn fd
  void DeleteSlave(const std::string& ip, int64_t port);
  int32_t CountSyncSlaves();
  int32_t GetSlaveListString(std::string& slave_list_str);
  int64_t TryAddSlave(const std::string& ip, int64_t port, int fd,
                      const std::vector<TableStruct>& table_structs);
  //Status AddBinlogSender(const std::string& table_name,
  //                       uint32_t partition_id,
  //                       const std::string& ip, int64_t port,
  //                       int64_t sid,
  //                       uint32_t filenum, uint64_t con_offset);
  slash::Mutex slave_mutex_; // protect slaves_;
  std::vector<SlaveItem> slaves_;


  /*
   * Slave use
   */
  void SyncError();
  void RemoveMaster();
  bool ShouldStartPingMaster();
  bool SetMaster(std::string& master_ip, int master_port);

  /*
   * Slave State Machine
   */
  bool ShouldMetaSync();
  void MetaSyncDone();
  bool ShouldWaitMetaSyncResponse();
  void CheckWaitMetaSyncTimeout();
  bool ShouldMarkTryConnect();
  void MarkTryConnectDone();
  bool ShouldTrySyncPartition();
  void MarkEstablishSuccess();
  void ResetMetaSyncStatus();

  /*
   * ThreadPool Process Task
   */
  void Schedule(pink::TaskFunc func, void* arg);

  /*
   * BGSave used
   */
  void BGSaveTaskSchedule(pink::TaskFunc func, void* arg);

  /*
   * PurgeLog used
   */
  void PurgelogsTaskSchedule(pink::TaskFunc func, void* arg);

  /*
   * Flushall & Flushdb used
   */
  void PurgeDir(const std::string& path);
  void PurgeDirTaskSchedule(void (*function)(void*), void* arg);

  /*
   * DBSync used
   */
  void DBSync(const std::string& ip, int port,
              const std::string& table_name,
              uint32_t partition_id);
  void TryDBSync(const std::string& ip, int port,
                 const std::string& table_name,
                 uint32_t partition_id, int32_t top);
  void DbSyncSendFile(const std::string& ip, int port,
                      const std::string& table_name,
                      uint32_t partition_id);
  std::string DbSyncTaskIndex(const std::string& ip, int port,
                              const std::string& table_name,
                              uint32_t partition_id);

  /*
   * Keyscan used
   */
  void KeyScanTaskSchedule(pink::TaskFunc func, void* arg);

  /*
   * Client used
   */
  void ClientKillAll();
  int ClientKill(const std::string &ip_port);
  int64_t ClientList(std::vector<ClientInfo> *clients = nullptr);

  /*
   * Monitor used
   */
  bool HasMonitorClients();
  void AddMonitorMessage(const std::string &monitor_message);
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
  void UpdateQueryNumAndExecCountTable(const std::string& command);
  std::unordered_map<std::string, uint64_t> ServerExecCountTable();

  /*
   * Slave to Master communication used
   */
  int SendToPeer();
  void SignalAuxiliary();
  Status TriggerSendBinlogSync();
  Status SendMetaSyncRequest();
  Status SendPartitionDBSyncRequest(std::shared_ptr<Partition> partition);
  Status SendPartitionTrySyncRequest(std::shared_ptr<Partition> partition);
  Status SendPartitionBinlogSyncAckRequest(const std::string& table, uint32_t partition_id,
                                           const BinlogOffset& ack_start, const BinlogOffset& ack_end,
                                           bool is_frist_send = false);

  /*
   * PubSub used
   */
  int PubSubNumPat();
  int Publish(const std::string& channel, const std::string& msg);
  int UnSubscribe(std::shared_ptr<pink::PinkConn> conn,
                  const std::vector<std::string>& channels,
                  const bool pattern,
                  std::vector<std::pair<std::string, int>>* result);
  void Subscribe(std::shared_ptr<pink::PinkConn> conn,
                 const std::vector<std::string>& channels,
                 const bool pattern,
                 std::vector<std::pair<std::string, int>>* result);
  void PubSubChannels(const std::string& pattern,
                      std::vector<std::string>* result);
  void PubSubNumSub(const std::vector<std::string>& channels,
                    std::vector<std::pair<std::string, int>>* result);

  friend class Cmd;
  friend class InfoCmd;
  friend class PikaReplClientConn;

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
  int port_;
  time_t start_time_s_;

  std::atomic<bool> exit_;

  /*
   * Table used
   */
  pthread_rwlock_t tables_rw_;
  std::map<std::string, std::shared_ptr<Table>> tables_;

  /*
   * CronTask used
   */
  bool have_scheduled_crontask_;
  struct timeval last_check_compact_time_;

  /*
   * Communicate with the client used
   */
  int worker_num_;
  pink::ThreadPool* pika_thread_pool_;
  PikaDispatchThread* pika_dispatch_thread_;


  /*
   * Master used
   */
  int64_t sid_;

  /*
   * Slave used
   */
  std::string master_ip_;
  int master_port_;
  int repl_state_;
  int role_;
  int last_meta_sync_timestamp_;
  bool force_full_sync_;
  pthread_rwlock_t state_protector_; //protect below, use for master-slave mode

  /*
   * Bgsave used
   */
  pink::BGThread bgsave_thread_;

  /*
   * Purgelogs use
   */
  pink::BGThread purge_thread_;

  /*
   * DBSync used
   */
  slash::Mutex db_sync_protector_;
  std::unordered_set<std::string> db_sync_slaves_;

  /*
   * Keyscan used
   */
  pink::BGThread key_scan_thread_;

  /*
   * Monitor used
   */
  PikaMonitorThread* pika_monitor_thread_;

  /*
   * Rsync used
   */
  PikaRsyncService* pika_rsync_service_;

  /*
   * Pubsub used
   */
  pink::PubSubThread* pika_pubsub_thread_;

  /*
   * Communication used
   */
  PikaAuxiliaryThread* pika_auxiliary_thread_;

  /*
   * Slowlog used
   */
  uint64_t slowlog_entry_id_;
  pthread_rwlock_t slowlog_protector_;
  std::list<SlowlogEntry> slowlog_list_;

  /*
   * Statistic used
   */
  StatisticData statistic_data_;

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};

#endif
