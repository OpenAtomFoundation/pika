// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_RM_H_
#define PIKA_RM_H_

#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "pstd/include/pstd_status.h"

#include "include/pika_binlog_reader.h"
#include "include/pika_consensus.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"
#include "include/rsync_client.h"

#define kBinlogSendPacketNum 40
#define kBinlogSendBatchNum 100

// unit seconds
#define kSendKeepAliveTimeout (2 * 1000000)
#define kRecvKeepAliveTimeout (20 * 1000000)


class SyncSlot {
 public:
  SyncSlot(const std::string& db_name, uint32_t slot_id);
  virtual ~SyncSlot() = default;

  SlotInfo& SyncSlotInfo() { return slot_info_; }

  std::string SlotName();

 protected:
  SlotInfo slot_info_;
};

class SyncMasterSlot : public SyncSlot {
 public:
  SyncMasterSlot(const std::string& db_name, uint32_t slot_id);
  pstd::Status AddSlaveNode(const std::string& ip, int port, int session_id);
  pstd::Status RemoveSlaveNode(const std::string& ip, int port);

  pstd::Status ActivateSlaveBinlogSync(const std::string& ip, int port, const LogOffset& offset);
  pstd::Status ActivateSlaveDbSync(const std::string& ip, int port);

  pstd::Status SyncBinlogToWq(const std::string& ip, int port);

  pstd::Status GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset, BinlogOffset* acked_offset);
  pstd::Status GetSlaveState(const std::string& ip, int port, SlaveState* slave_state);

  pstd::Status SetLastSendTime(const std::string& ip, int port, uint64_t time);
  pstd::Status GetLastSendTime(const std::string& ip, int port, uint64_t* time);

  pstd::Status SetLastRecvTime(const std::string& ip, int port, uint64_t time);
  pstd::Status GetLastRecvTime(const std::string& ip, int port, uint64_t* time);

  pstd::Status GetSafetyPurgeBinlog(std::string* safety_purge);
  bool BinlogCloudPurge(uint32_t index);

  pstd::Status WakeUpSlaveBinlogSync();
  pstd::Status CheckSyncTimeout(uint64_t now);

  int GetNumberOfSlaveNode();
  bool CheckSlaveNodeExist(const std::string& ip, int port);
  pstd::Status GetSlaveNodeSession(const std::string& ip, int port, int32_t* session);

  void GetValidSlaveNames(std::vector<std::string>* slavenames);
  // display use
  pstd::Status GetInfo(std::string* info);
  // debug use
  std::string ToStringStatus();

  int32_t GenSessionId();
  bool CheckSessionId(const std::string& ip, int port, const std::string& db_name, uint64_t slot_id,
                      int session_id);

  // consensus use
  pstd::Status ConsensusUpdateSlave(const std::string& ip, int port, const LogOffset& start, const LogOffset& end);
  pstd::Status ConsensusProposeLog(const std::shared_ptr<Cmd>& cmd_ptr);
  pstd::Status ConsensusSanityCheck();
  pstd::Status ConsensusProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute);
  LogOffset ConsensusCommittedIndex();
  LogOffset ConsensusLastIndex();
  uint32_t ConsensusTerm();
  void ConsensusUpdateTerm(uint32_t term);
  pstd::Status ConsensusUpdateAppliedIndex(const LogOffset& offset);
  LogOffset ConsensusAppliedIndex();
  pstd::Status ConsensusLeaderNegotiate(const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints);
  pstd::Status ConsensusFollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset);
  pstd::Status ConsensusReset(const LogOffset& applied_offset);
  void CommitPreviousLogs(const uint32_t& term);

  std::shared_ptr<StableLog> StableLogger() { return coordinator_.StableLogger(); }

  std::shared_ptr<Binlog> Logger() {
    if (!coordinator_.StableLogger()) {
      return nullptr;
    }
    return coordinator_.StableLogger()->Logger();
  }

 private:
  bool CheckReadBinlogFromCache();
  // invoker need to hold slave_mu_
  pstd::Status ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr);

  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();

  pstd::Mutex session_mu_;
  int32_t session_id_ = 0;

  ConsensusCoordinator coordinator_;
};

class SyncSlaveSlot : public SyncSlot {
 public:
  SyncSlaveSlot(const std::string& db_name, uint32_t slot_id);

  void Activate(const RmNode& master, const ReplState& repl_state);
  void Deactivate();

  void SetLastRecvTime(uint64_t time);
  uint64_t LastRecvTime();

  void SetReplState(const ReplState& repl_state);
  ReplState State();

  pstd::Status CheckSyncTimeout(uint64_t now);

  // For display
  pstd::Status GetInfo(std::string* info);
  // For debug
  std::string ToStringStatus();

  const std::string& MasterIp();

  int MasterPort();

  void SetMasterSessionId(int32_t session_id);

  int32_t MasterSessionId();

  void SetLocalIp(const std::string& local_ip);

  std::string LocalIp();

  void StopRsync();

  void ActivateRsync();

  bool IsRsyncRunning() {return rsync_cli_->IsRunning();}

 private:
  std::unique_ptr<rsync::RsyncClient> rsync_cli_;
  pstd::Mutex slot_mu_;
  RmNode m_info_;
  ReplState repl_state_{kNoConnect};
  std::string local_ip_;
};

class PikaReplicaManager {
 public:
  PikaReplicaManager();
  ~PikaReplicaManager() = default;

  friend Cmd;

  void Start();
  void Stop();

  bool CheckMasterSyncFinished();

  pstd::Status AddSyncSlotSanityCheck(const std::set<SlotInfo>& p_infos);
  pstd::Status AddSyncSlot(const std::set<SlotInfo>& p_infos);
  pstd::Status RemoveSyncSlotSanityCheck(const std::set<SlotInfo>& p_infos);
  pstd::Status RemoveSyncSlot(const std::set<SlotInfo>& p_infos);
  pstd::Status ActivateSyncSlaveSlot(const RmNode& node, const ReplState& repl_state);
  pstd::Status DeactivateSyncSlaveSlot(const SlotInfo& p_info);
  pstd::Status SyncDBSanityCheck(const std::string& db_name);
  pstd::Status DelSyncDB(const std::string& db_name);

  // For Pika Repl Client Thread
  pstd::Status SendMetaSyncRequest();
  pstd::Status SendRemoveSlaveNodeRequest(const std::string& table, uint32_t slot_id);
  pstd::Status SendSlotTrySyncRequest(const std::string& db_name, size_t slot_id);
  pstd::Status SendSlotDBSyncRequest(const std::string& db_name, size_t slot_id);
  pstd::Status SendSlotBinlogSyncAckRequest(const std::string& table, uint32_t slot_id, const LogOffset& ack_start,
                                           const LogOffset& ack_end, bool is_first_send = false);
  pstd::Status CloseReplClientConn(const std::string& ip, int32_t port);

  // For Pika Repl Server Thread
  pstd::Status SendSlaveBinlogChipsRequest(const std::string& ip, int port, const std::vector<WriteTask>& tasks);

  // For SyncMasterSlot
  std::shared_ptr<SyncMasterSlot> GetSyncMasterSlotByName(const SlotInfo& p_info);

  // For SyncSlaveSlot
  std::shared_ptr<SyncSlaveSlot> GetSyncSlaveSlotByName(const SlotInfo& p_info);

  pstd::Status RunSyncSlaveSlotStateMachine();

  pstd::Status CheckSyncTimeout(uint64_t now);

  // To check slot info
  // For pkcluster info command
  pstd::Status GetSlotInfo(const std::string& table, uint32_t slot_id, std::string* info);

  void FindCompleteReplica(std::vector<std::string>* replica);
  void FindCommonMaster(std::string* master);
  pstd::Status CheckSlotRole(const std::string& table, uint32_t slot_id, int* role);

  void RmStatus(std::string* debug_info);

  static bool CheckSlaveSlotState(const std::string& ip, int port);

  pstd::Status LostConnection(const std::string& ip, int port);

  // Update binlog win and try to send next binlog
  pstd::Status UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start, const LogOffset& offset_end);

  pstd::Status WakeUpBinlogSync();

  // write_queue related
  void ProduceWriteQueue(const std::string& ip, int port, uint32_t slot_id, const std::vector<WriteTask>& tasks);
  int ConsumeWriteQueue();
  void DropItemInWriteQueue(const std::string& ip, int port);

  // Schedule Task
  void ScheduleReplServerBGTask(net::TaskFunc func, void* arg);
  void ScheduleReplClientBGTask(net::TaskFunc func, void* arg);
  void ScheduleWriteBinlogTask(const std::string& db_slot,
                               const std::shared_ptr<InnerMessage::InnerResponse>& res,
                               const std::shared_ptr<net::PbConn>& conn, void* res_private_data);
  void ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const LogOffset& offset, const std::string& db_name,
                           uint32_t slot_id);

  void ReplServerRemoveClientConn(int fd);
  void ReplServerUpdateClientConnMap(const std::string& ip_port, int fd);

  std::shared_mutex& GetSlotLock() { return slots_rw_; }
  void SlotLock() {
    slots_rw_.lock();
  }
  void SlotLockShared() {
    slots_rw_.lock_shared();
  }
  void SlotUnlock() {
    slots_rw_.unlock();
  }
  void SlotUnlockShared() {
    slots_rw_.unlock_shared();
  }

  std::unordered_map<SlotInfo, std::shared_ptr<SyncMasterSlot>, hash_slot_info>& GetSyncMasterSlots() {
    return sync_master_slots_;
  }
  std::unordered_map<SlotInfo, std::shared_ptr<SyncSlaveSlot>, hash_slot_info>& GetSyncSlaveSlots() {
    return sync_slave_slots_;
  }

 private:
  void InitSlot();
  pstd::Status SelectLocalIp(const std::string& remote_ip, int remote_port, std::string* local_ip);

  std::shared_mutex slots_rw_;
  std::unordered_map<SlotInfo, std::shared_ptr<SyncMasterSlot>, hash_slot_info> sync_master_slots_;
  std::unordered_map<SlotInfo, std::shared_ptr<SyncSlaveSlot>, hash_slot_info> sync_slave_slots_;

  pstd::Mutex write_queue_mu_;
  // every host owns a queue, the key is "ip+port"
  std::unordered_map<std::string, std::unordered_map<uint32_t, std::queue<WriteTask>>> write_queues_;

  std::unique_ptr<PikaReplClient> pika_repl_client_;
  std::unique_ptr<PikaReplServer> pika_repl_server_;
};

#endif  //  PIKA_RM_H
