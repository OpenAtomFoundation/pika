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
#include "include/pika_monitor_thread.h"
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

extern PikaConf* g_pika_conf;

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

enum BlockPopType { Blpop, Brpop };
typedef struct blrpopKey{  // this data struct is made for the scenario of multi dbs in pika.
  std::string db_name;
  std::string key;
  bool operator==(const blrpopKey& p) const{
    return p.db_name == db_name && p.key == key;
  }
} BlrPopKey;
struct BlrPopKeyHash {
  std::size_t operator()(const BlrPopKey& k) const {
    return std::hash<std::string>{}(k.db_name) ^ std::hash<std::string>{}(k.key);
  }
};


class BlockedPopConnNode {
 public:
  virtual ~BlockedPopConnNode() {
    std::cout << "BlockedPopConnNode: fd-" << conn_blocked_->fd() << " expire_time_:" << expire_time_ << std::endl;
  }
  BlockedPopConnNode(int64_t expire_time, std::shared_ptr<PikaClientConn>& conn_blocked, BlockPopType block_type)
      : expire_time_(expire_time), conn_blocked_(conn_blocked), block_type_(block_type) {}
  bool IsExpired() {
    if (expire_time_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (expire_time_ <= unix_time) {
      return true;
    }
    return false;
  }
  std::shared_ptr<PikaClientConn>& GetConnBlocked() { return conn_blocked_; }
  BlockPopType GetBlockType() const { return block_type_; }

  void SelfPrint() {
    std::cout << "fd:" << conn_blocked_->fd() << ", expire_time:" << expire_time_ << ", blockType: " << block_type_
              << std::endl;
  }

 private:
  int64_t expire_time_;
  std::shared_ptr<PikaClientConn> conn_blocked_;
  BlockPopType block_type_;
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
  bool HasMonitorClients();
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

  /**
   * BlPop/BrPop used
   */

  void CleanWaitInfoOfUnBlockedBlrConn(std::shared_ptr<PikaClientConn> conn_unblocked) {
    // removed all the waiting info of this conn/ doing cleaning work
    auto& blpop_keys_list = map_from_conns_to_keys_for_blrpop.find(conn_unblocked->fd())->second;
    for (auto& blpop_key : *blpop_keys_list) {
      auto& wait_list_of_this_key = map_from_keys_to_conns_for_blrpop.find(blpop_key)->second;
      for (auto conn = wait_list_of_this_key->begin(); conn != wait_list_of_this_key->end();) {
        if (conn->GetConnBlocked()->fd() == conn_unblocked->fd()) {
          conn = wait_list_of_this_key->erase(conn);
          break;
        }
        conn++;
      }
    }
    map_from_conns_to_keys_for_blrpop.erase(conn_unblocked->fd());
  }

  void CleanKeysAfterWaitInfoCleaned(std::string table_name) {
    // after wait info of a conn is cleaned, some wait list of keys might be empty, must erase them from the map
    std::vector<BlrPopKey> keys_to_erase;
    for (auto& pair : map_from_keys_to_conns_for_blrpop) {
      if (pair.second->empty()) {
        // wait list of this key is empty, just erase this key
        keys_to_erase.emplace_back(pair.first);
      }
    }
    for (auto& blrpop_key : keys_to_erase) {
      map_from_keys_to_conns_for_blrpop.erase(blrpop_key);
    }
  }

  void BlockThisClientToWaitLRPush(std::shared_ptr<PikaClientConn> conn_to_block, std::vector<std::string>& keys,
                                   int64_t expire_time, BlockPopType block_pop_type) {
    std::lock_guard latch(bLRPop_blocking_map_latch_);
    std::vector<BlrPopKey> blrpop_keys;
    for (auto& key : keys) {
      BlrPopKey blrpop_key{conn_to_block->GetCurrentTable(), key};
      blrpop_keys.push_back(blrpop_key);
      auto it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
      if (it == map_from_keys_to_conns_for_blrpop.end()) {
        // no waiting info found, means no other clients are waiting for the list related with this key right now
        map_from_keys_to_conns_for_blrpop.emplace(blrpop_key, std::make_unique<std::list<BlockedPopConnNode>>());
        it = map_from_keys_to_conns_for_blrpop.find(blrpop_key);
      }
      auto& wait_list_of_this_key = it->second;
      // add current client-connection to the tail of waiting list of this key
      wait_list_of_this_key->emplace_back(expire_time, conn_to_block, block_pop_type);
    }

    // construct a list of keys and insert into this map as value(while key of the map is conn_fd)
    map_from_conns_to_keys_for_blrpop.emplace(
        conn_to_block->fd(), std::make_unique<std::list<BlrPopKey>>(blrpop_keys.begin(), blrpop_keys.end()));

    std::cout << "-------------db name:" << conn_to_block->GetCurrentTable() << "-------------" << std::endl;
    std::cout << "from key to conn:" << std::endl;
    for (auto& pair : map_from_keys_to_conns_for_blrpop) {
      std::cout << "key:<" << pair.first.db_name << "," << pair.first.key << ">  list of it:" << std::endl;
      for (auto& it : *pair.second) {
        it.SelfPrint();
      }
    }

    std::cout << "\n\nfrom conn to key:" << std::endl;
    for (auto& pair : map_from_conns_to_keys_for_blrpop) {
      std::cout << "fd:" << pair.first << "  related keys:" << std::endl;
      for (auto& it : *pair.second) {
        std::cout << " <" << it.db_name << "," << it.key << "> " << std::endl;
      }
    }
    std::cout << "-----------end------------------" << std::endl;
  }

  void TryToServeBLrPopWithThisKey(const std::string& key, const std::string& table_name,
                                   std::shared_ptr<Partition> partition) {
    std::lock_guard latch(bLRPop_blocking_map_latch_);
    BlrPopKey blrPop_key{table_name, key};
    auto it = map_from_keys_to_conns_for_blrpop.find(blrPop_key);
    if (it == map_from_keys_to_conns_for_blrpop.end()) {
      // no client is waitting for this key
      return;
    }

    auto& waitting_list_of_this_key = it->second;
    std::string value;
    rocksdb::Status s;
    // traverse this list from head to tail(in the order of adding sequence) which means "first blocked, first get
    // served“
    CmdRes res;
    for (auto conn_blocked = waitting_list_of_this_key->begin(); conn_blocked != waitting_list_of_this_key->end();) {
      auto conn_ptr = conn_blocked->GetConnBlocked();

      if (conn_blocked->GetBlockType() == BlockPopType::Blpop) {
        s = partition->db()->LPop(key, &value);
      } else {  // BlockPopType is Brpop
        s = partition->db()->RPop(key, &value);
      }

      if (s.ok()) {
        res.AppendArrayLen(2);
        res.AppendString(key);
        res.AppendString(value);
      } else if (s.IsNotFound()) {
        // this key has no more elements to serve more blocked conn.
        break;
      } else {
        res.SetRes(CmdRes::kErrOther, s.ToString());
      }
      // send response to this client
      conn_ptr->WriteResp(res.message());
      res.clear();
      conn_ptr->NotifyEpoll(true);
      conn_blocked = waitting_list_of_this_key->erase(conn_blocked);  // remove this conn from current waiting list
      // erase all waiting info of this conn
      CleanWaitInfoOfUnBlockedBlrConn(conn_ptr);
    }
    CleanKeysAfterWaitInfoCleaned(table_name);
  }

  // if a client closed the conn when waiting for the response of "blpop/brpop", some cleaning work must be done.
  void ClosingConnCheckForBlrPop(std::shared_ptr<net::NetConn> conn_to_close) {
    std::shared_ptr<PikaClientConn> conn = std::dynamic_pointer_cast<PikaClientConn>(conn_to_close);
    if (!conn) {
      // it's not an instance of PikaClientConn, no need of the process below
      return;
    }
    std::lock_guard l(bLRPop_blocking_map_latch_);
    auto keys_list = map_from_conns_to_keys_for_blrpop.find(conn->fd());
    if (keys_list == map_from_conns_to_keys_for_blrpop.end()) {
      // this conn is not disconnected from with blocking state cause by "blpop/brpop"
      return;
    }
    CleanWaitInfoOfUnBlockedBlrConn(conn);
    CleanKeysAfterWaitInfoCleaned(conn->GetCurrentTable());
  }

  /*  std::mutex& GetBLRPopBlockingMapLatch() { return bLRPop_blocking_map_latch_; }

    std::unique_ptr<std::unordered_map<int, std::unique_ptr<std::list<std::string>>>>& GetMapFromConnsToKeysForBlrpop(){
        map_from_conns_to_keys_for_blrpop;
    };*/

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
   *  Blpop/BRpop used
   */
  /*  map_from_keys_to_conns_for_blrpop:
   *  mapping from "Blrpopkey"(eg. "<db0, list1>") to a list that stored the blocking info of client-connetions that
   * were blocked by command blpop/brpop with key (eg. "list1").
   */

  std::unordered_map<BlrPopKey, std::unique_ptr<std::list<BlockedPopConnNode>>, BlrPopKeyHash> map_from_keys_to_conns_for_blrpop;

  /*
   *  map_from_conns_to_keys_for_blrpop:
   *  mapping from conn(fd) to a list of keys that the client is waiting for.
   */
  std::unordered_map<int, std::unique_ptr<std::list<BlrPopKey>>> map_from_conns_to_keys_for_blrpop;

  /*
   * latch of the two maps above.
   */
  std::mutex bLRPop_blocking_map_latch_;

  /*
   * CronTask used
   */
  bool have_scheduled_crontask_ = false;
  struct timeval last_check_compact_time_;

  /*
   * Communicate with the client used
   */
  int worker_num_ = 0;
  PikaClientProcessor* pika_client_processor_ = nullptr;
  PikaDispatchThread* pika_dispatch_thread_ = nullptr;

  /*
   * Slave used
   */
  std::string master_ip_;
  int master_port_ = 0;
  int repl_state_ = PIKA_REPL_NO_CONNECT;
  int role_ = PIKA_ROLE_SINGLE;
  int last_meta_sync_timestamp_ = 0;
  bool first_meta_sync_ = false;
  bool loop_partition_state_machine_ = false;
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
  PikaMonitorThread* pika_monitor_thread_ = nullptr;

  /*
   * Rsync used
   */
  PikaRsyncService* pika_rsync_service_ = nullptr;

  /*
   * Pubsub used
   */
  net::PubSubThread* pika_pubsub_thread_ = nullptr;

  /*
   * Communication used
   */
  PikaAuxiliaryThread* pika_auxiliary_thread_ = nullptr;

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
