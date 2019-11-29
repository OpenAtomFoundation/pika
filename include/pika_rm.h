// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RM_H_
#define PIKA_RM_H_

#include <string>
#include <memory>
#include <unordered_map>
#include <queue>
#include <vector>

#include "slash/include/slash_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_stable_log.h"
#include "include/pika_consistency.h"
#include "include/pika_slave_node.h"

#define kBinlogSendPacketNum 40
#define kBinlogSendBatchNum 100

// unit seconds
#define kSendKeepAliveTimeout (10 * 1000000)
#define kRecvKeepAliveTimeout (20 * 1000000)

using slash::Status;

class SyncPartition {
 public:
  SyncPartition(const std::string& table_name, uint32_t partition_id);
  virtual ~SyncPartition() = default;

  PartitionInfo& SyncPartitionInfo() {
    return partition_info_;
  }

  std::string PartitionName();

 protected:
  PartitionInfo partition_info_;
};

class SyncMasterPartition : public SyncPartition {
 public:
  SyncMasterPartition(const std::string& table_name, uint32_t partition_id);
  Status AddSlaveNode(const std::string& ip, int port, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);

  Status ActivateSlaveBinlogSync(const std::string& ip, int port, const BinlogOffset& offset);
  Status ActivateSlaveDbSync(const std::string& ip, int port);

  Status SyncBinlogToWq(const std::string& ip, int port);
  Status UpdateSlaveBinlogAckInfo(const std::string& ip, int port, const BinlogOffset& start, const BinlogOffset& end);
  Status GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset, BinlogOffset* acked_offset);
  Status GetSlaveState(const std::string& ip, int port, SlaveState* const slave_state);

  Status SetLastSendTime(const std::string& ip, int port, uint64_t time);
  Status GetLastSendTime(const std::string& ip, int port, uint64_t* time);

  Status SetLastRecvTime(const std::string& ip, int port, uint64_t time);
  Status GetLastRecvTime(const std::string& ip, int port, uint64_t* time);

  Status GetSafetyPurgeBinlog(std::string* safety_purge);
  bool BinlogCloudPurge(uint32_t index);

  Status WakeUpSlaveBinlogSync();
  Status CheckSyncTimeout(uint64_t now);

  int GetNumberOfSlaveNode();
  bool CheckSlaveNodeExist(const std::string& ip, int port);
  Status GetSlaveNodeSession(const std::string& ip, int port, int32_t* session);

  void GetValidSlaveNames(std::vector<std::string>* slavenames);
  // display use
  Status GetInfo(std::string* info);
  // debug use
  std::string ToStringStatus();

  int32_t GenSessionId();
  bool    CheckSessionId(const std::string& ip, int port,
                         const std::string& table_name,
                         uint64_t partition_id, int session_id);

  // consistency use
  Status ConsistencyProposeLog(
      const BinlogOffset& offset,
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status ConsistencySanityCheck();
  Status ConsistencyScheduleApplyLog();

  std::shared_ptr<StableLog> StableLogger() {
    return coordinator_.StableLogger();
  }

  std::shared_ptr<Binlog> Logger() {
    if (!coordinator_.StableLogger()) {
      return nullptr;
    }
    return coordinator_.StableLogger()->Logger();
  }

 private:
  bool CheckReadBinlogFromCache();
  // invoker need to hold slave_mu_
  Status ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr);

  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();

  slash::Mutex session_mu_;
  int32_t session_id_;

  ConsistencyCoordinator coordinator_;
};

class SyncSlavePartition : public SyncPartition {
 public:
  SyncSlavePartition(const std::string& table_name, uint32_t partition_id);

  void Activate(const RmNode& master, const ReplState& repl_state);
  void Deactivate();

  void SetLastRecvTime(uint64_t time);
  uint64_t LastRecvTime();

  void SetReplState(const ReplState& repl_state);
  ReplState State();

  Status CheckSyncTimeout(uint64_t now);

  // For display
  Status GetInfo(std::string* info);
  // For debug
  std::string ToStringStatus();

  const std::string& MasterIp();

  int MasterPort();

  void SetMasterSessionId(int32_t session_id);

  int32_t MasterSessionId();

  void SetLocalIp(const std::string& local_ip);

  std::string LocalIp();

 private:
  slash::Mutex partition_mu_;
  RmNode m_info_;
  ReplState repl_state_;
  std::string local_ip_;
};

class PikaReplicaManager {
 public:
  PikaReplicaManager();
  ~PikaReplicaManager();

  friend Cmd;

  void Start();
  void Stop();

  Status AddSyncPartitionSanityCheck(const std::set<PartitionInfo>& p_infos);
  Status AddSyncPartition(const std::set<PartitionInfo>& p_infos);
  Status RemoveSyncPartitionSanityCheck(const std::set<PartitionInfo>& p_infos);
  Status RemoveSyncPartition(const std::set<PartitionInfo>& p_infos);
  Status ActivateSyncSlavePartition(const RmNode& node, const ReplState& repl_state);
  Status DeactivateSyncSlavePartition(const PartitionInfo& p_info);

  // For Pika Repl Client Thread
  Status SendMetaSyncRequest();
  Status SendRemoveSlaveNodeRequest(const std::string& table, uint32_t partition_id);
  Status SendPartitionTrySyncRequest(const std::string& table_name, size_t partition_id);
  Status SendPartitionDBSyncRequest(const std::string& table_name, size_t partition_id);
  Status SendPartitionBinlogSyncAckRequest(const std::string& table, uint32_t partition_id,
                                           const BinlogOffset& ack_start, const BinlogOffset& ack_end,
                                           bool is_first_send = false);
  Status CloseReplClientConn(const std::string& ip, int32_t port);

  // For Pika Repl Server Thread
  Status SendSlaveBinlogChipsRequest(const std::string& ip, int port, const std::vector<WriteTask>& tasks);

  // For SyncMasterPartition
  std::shared_ptr<SyncMasterPartition> GetSyncMasterPartitionByName(const PartitionInfo& p_info);

  // For SyncSlavePartition
  std::shared_ptr<SyncSlavePartition> GetSyncSlavePartitionByName(const PartitionInfo& p_info);

  Status RunSyncSlavePartitionStateMachine();

  Status CheckSyncTimeout(uint64_t now);

  // To check partition info
  // For pkcluster info command
  Status GetPartitionInfo(
      const std::string& table, uint32_t partition_id, std::string* info);

  void FindCompleteReplica(std::vector<std::string>* replica);
  void FindCommonMaster(std::string* master);
  Status CheckPartitionRole(
      const std::string& table, uint32_t partition_id, int* role);

  void RmStatus(std::string* debug_info);

  Status LostConnection(const std::string& ip, int port);

  // Update binlog win and try to send next binlog
  Status UpdateSyncBinlogStatus(const RmNode& slave, const BinlogOffset& offset_start, const BinlogOffset& offset_end);

  Status WakeUpBinlogSync();

  // write_queue related
  void ProduceWriteQueue(const std::string& ip, int port, const std::vector<WriteTask>& tasks);
  int ConsumeWriteQueue();
  void DropItemInWriteQueue(const std::string& ip, int port);

  // Schedule Task
  void ScheduleReplServerBGTask(pink::TaskFunc func, void* arg);
  void ScheduleReplClientBGTask(pink::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(const std::string& table_partition,
                               const std::shared_ptr<InnerMessage::InnerResponse> res,
                               std::shared_ptr<pink::PbConn> conn, void* res_private_data);
  void ScheduleWriteDBTask(const std::string& dispatch_key,
                           PikaCmdArgsType* argv, BinlogItem* binlog_item,
                           const std::string& table_name, uint32_t partition_id);

  void ReplServerRemoveClientConn(int fd);
  void ReplServerUpdateClientConnMap(const std::string& ip_port, int fd);

 private:
  void InitPartition();
  Status SelectLocalIp(const std::string& remote_ip,
                       const int remote_port,
                       std::string* const local_ip);

  pthread_rwlock_t partitions_rw_;
  std::unordered_map<PartitionInfo, std::shared_ptr<SyncMasterPartition>, hash_partition_info> sync_master_partitions_;
  std::unordered_map<PartitionInfo, std::shared_ptr<SyncSlavePartition>, hash_partition_info> sync_slave_partitions_;

  slash::Mutex  write_queue_mu_;
  // every host owns a queue
  std::unordered_map<std::string, std::queue<WriteTask>> write_queues_;  // ip+port, queue<WriteTask>

  PikaReplClient* pika_repl_client_;
  PikaReplServer* pika_repl_server_;
  int last_meta_sync_timestamp_;
};

#endif  //  PIKA_RM_H
