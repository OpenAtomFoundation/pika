// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rm.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <utility>

#include "net/include/net_cli.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"

#include "include/pika_admin.h"
#include "include/pika_command.h"

using pstd::Status;

extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern PikaServer* g_pika_server;

/* SyncSlot */

SyncSlot::SyncSlot(const std::string& db_name, uint32_t slot_id)
    : slot_info_(db_name, slot_id) {}

std::string SyncSlot::SlotName() {
  return slot_info_.db_name_;
}

/* SyncMasterSlot*/

SyncMasterSlot::SyncMasterSlot(const std::string& db_name, uint32_t slot_id)
    : SyncSlot(db_name, slot_id),  coordinator_(db_name, slot_id) {}

int SyncMasterSlot::GetNumberOfSlaveNode() { return coordinator_.SyncPros().SlaveSize(); }

bool SyncMasterSlot::CheckSlaveNodeExist(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  return static_cast<bool>(slave_ptr);
}

Status SyncMasterSlot::GetSlaveNodeSession(const std::string& ip, int port, int32_t* session) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("slave " + ip + ":" + std::to_string(port) + " not found");
  }

  slave_ptr->Lock();
  *session = slave_ptr->SessionId();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = coordinator_.AddSlaveNode(ip, port, session_id);
  if (!s.ok()) {
    LOG(WARNING) << "Add Slave Node Failed, slot: " << SyncSlotInfo().ToString() << ", ip_port: " << ip << ":"
                 << port;
    return s;
  }
  LOG(INFO) << "Add Slave Node, slot: " << SyncSlotInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();
}

Status SyncMasterSlot::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = coordinator_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    LOG(WARNING) << "Remove Slave Node Failed, Slot: " << SyncSlotInfo().ToString() << ", ip_port: " << ip
                 << ":" << port;
    return s;
  }
  LOG(INFO) << "Remove Slave Node, Slot: " << SyncSlotInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();
}

Status SyncMasterSlot::ActivateSlaveBinlogSync(const std::string& ip, int port, const LogOffset& offset) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  {
    std::lock_guard l(slave_ptr->slave_mu);
    slave_ptr->slave_state = kSlaveBinlogSync;
    slave_ptr->sent_offset = offset;
    slave_ptr->acked_offset = offset;
    // read binlog file from file
    Status s = slave_ptr->InitBinlogFileReader(Logger(), offset.b_offset);
    if (!s.ok()) {
      return Status::Corruption("Init binlog file reader failed" + s.ToString());
    }
    //Since we init a new reader, we should drop items in write queue and reset sync_window.
    //Or the sent_offset and acked_offset will not match
    g_pika_rm->DropItemInWriteQueue(ip, port);
    slave_ptr->sync_win.Reset();
    slave_ptr->b_state = kReadFromFile;
  }

  Status s = SyncBinlogToWq(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status SyncMasterSlot::SyncBinlogToWq(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }
  Status s;
  slave_ptr->Lock();
  s = ReadBinlogFileToWq(slave_ptr);
  slave_ptr->Unlock();
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status SyncMasterSlot::ActivateSlaveDbSync(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->slave_state = kSlaveDbSync;
  // invoke db sync
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
  int cnt = slave_ptr->sync_win.Remaining();
  std::shared_ptr<PikaBinlogReader> reader = slave_ptr->binlog_reader;
  if (!reader) {
    return Status::OK();
  }
  std::vector<WriteTask> tasks;
  for (int i = 0; i < cnt; ++i) {
    std::string msg;
    uint32_t filenum;
    uint64_t offset;
    if (slave_ptr->sync_win.GetTotalBinlogSize() > PIKA_MAX_CONN_RBUF_HB * 2) {
      LOG(INFO) << slave_ptr->ToString()
                << " total binlog size in sync window is :" << slave_ptr->sync_win.GetTotalBinlogSize();
      break;
    }
    Status s = reader->Get(&msg, &filenum, &offset);
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(WARNING) << SyncSlotInfo().ToString() << " Read Binlog error : " << s.ToString();
      return s;
    }
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, msg, &item)) {
      LOG(WARNING) << "Binlog item decode failed";
      return Status::Corruption("Binlog item decode failed");
    }
    BinlogOffset sent_b_offset = BinlogOffset(filenum, offset);
    LogicOffset sent_l_offset = LogicOffset(item.term_id(), item.logic_id());
    LogOffset sent_offset(sent_b_offset, sent_l_offset);

    slave_ptr->sync_win.Push(SyncWinItem(sent_offset, msg.size()));
    slave_ptr->SetLastSendTime(pstd::NowMicros());
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SlotId(),
                   slave_ptr->SessionId());
    WriteTask task(rm_node, BinlogChip(sent_offset, msg), slave_ptr->sent_offset);
    tasks.push_back(task);
    slave_ptr->sent_offset = sent_offset;
  }

  if (!tasks.empty()) {
    g_pika_rm->ProduceWriteQueue(slave_ptr->Ip(), slave_ptr->Port(), slot_info_.slot_id_, tasks);
  }
  return Status::OK();
}

Status SyncMasterSlot::ConsensusUpdateSlave(const std::string& ip, int port, const LogOffset& start,
                                                 const LogOffset& end) {
  Status s = coordinator_.UpdateSlave(ip, port, start, end);
  if (!s.ok()) {
    LOG(WARNING) << SyncSlotInfo().ToString() << s.ToString();
    return s;
  }
  return Status::OK();
}

Status SyncMasterSlot::ConsensusUpdateAppliedIndex(const LogOffset& offset) {
  std::shared_ptr<Context> context = coordinator_.context();
  if (!context) {
    LOG(WARNING) << "Coordinator context empty.";
    return Status::NotFound("context");
  }
  context->UpdateAppliedIndex(offset);
  return Status::OK();
}

LogOffset SyncMasterSlot::ConsensusAppliedIndex() { return coordinator_.applied_index(); }

Status SyncMasterSlot::GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset,
                                                   BinlogOffset* acked_offset) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *sent_offset = slave_ptr->sent_offset.b_offset;
  *acked_offset = slave_ptr->acked_offset.b_offset;
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::GetSlaveState(const std::string& ip, int port, SlaveState* const slave_state) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *slave_state = slave_ptr->slave_state;
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::WakeUpSlaveBinlogSync() {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  std::vector<std::shared_ptr<SlaveNode>> to_del;
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);
    if (slave_ptr->sent_offset == slave_ptr->acked_offset) {
      Status s = ReadBinlogFileToWq(slave_ptr);
      if (!s.ok()) {
        to_del.push_back(slave_ptr);
        LOG(WARNING) << "WakeUpSlaveBinlogSync falied, Delete from RM, slave: " << slave_ptr->ToStringStatus() << " "
                     << s.ToString();
      }
    }
  }
  for (auto& to_del_slave : to_del) {
    RemoveSlaveNode(to_del_slave->Ip(), to_del_slave->Port());
  }
  return Status::OK();
}

Status SyncMasterSlot::SetLastSendTime(const std::string& ip, int port, uint64_t time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->SetLastSendTime(time);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::GetLastSendTime(const std::string& ip, int port, uint64_t* time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *time = slave_ptr->LastSendTime();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::SetLastRecvTime(const std::string& ip, int port, uint64_t time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->SetLastRecvTime(time);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::GetLastRecvTime(const std::string& ip, int port, uint64_t* time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *time = slave_ptr->LastRecvTime();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterSlot::GetSafetyPurgeBinlog(std::string* safety_purge) {
  BinlogOffset boffset;
  Status s = Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    return s;
  }
  bool success = false;
  uint32_t purge_max = boffset.filenum;
  if (purge_max >= 10) {
    success = true;
    purge_max -= 10;
    std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
    for (const auto& slave_iter : slaves) {
      std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
      std::lock_guard l(slave_ptr->slave_mu);
      if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync && slave_ptr->acked_offset.b_offset.filenum > 0) {
        purge_max = std::min(slave_ptr->acked_offset.b_offset.filenum - 1, purge_max);
      } else {
        success = false;
        break;
      }
    }
  }
  *safety_purge = (success ? kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none");
  return Status::OK();
}

bool SyncMasterSlot::BinlogCloudPurge(uint32_t index) {
  BinlogOffset boffset;
  Status s = Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    return false;
  }
  if (index > (boffset.filenum - 10)) {  // remain some more
    return false;
  } else {
    std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
    for (const auto& slave_iter : slaves) {
      std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
      std::lock_guard l(slave_ptr->slave_mu);
      if (slave_ptr->slave_state == SlaveState::kSlaveDbSync) {
        return false;
      } else if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync) {
        if (index >= slave_ptr->acked_offset.b_offset.filenum) {
          return false;
        }
      }
    }
  }
  return true;
}

Status SyncMasterSlot::CheckSyncTimeout(uint64_t now) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  std::vector<Node> to_del;
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);
    if (slave_ptr->LastRecvTime() + kRecvKeepAliveTimeout < now) {
      to_del.emplace_back(slave_ptr->Ip(), slave_ptr->Port());
    } else if (slave_ptr->LastSendTime() + kSendKeepAliveTimeout < now &&
               slave_ptr->sent_offset == slave_ptr->acked_offset) {
      std::vector<WriteTask> task;
      RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SlotId(),
                     slave_ptr->SessionId());
      WriteTask empty_task(rm_node, BinlogChip(LogOffset(), ""), LogOffset());
      task.push_back(empty_task);
      Status s = g_pika_rm->SendSlaveBinlogChipsRequest(slave_ptr->Ip(), slave_ptr->Port(), task);
      slave_ptr->SetLastSendTime(now);
      if (!s.ok()) {
        LOG(INFO) << "Send ping failed: " << s.ToString();
        return Status::Corruption("Send ping failed: " + slave_ptr->Ip() + ":" + std::to_string(slave_ptr->Port()));
      }
    }
  }

  for (auto& node : to_del) {
    coordinator_.SyncPros().RemoveSlaveNode(node.Ip(), node.Port());
    g_pika_rm->DropItemInWriteQueue(node.Ip(), node.Port());
    LOG(WARNING) << SyncSlotInfo().ToString() << " Master del Recv Timeout slave success " << node.ToString();
  }
  return Status::OK();
}

std::string SyncMasterSlot::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << " Current Master Session: " << session_id_ << "\r\n";
  tmp_stream << "  Consensus: "
             << "\r\n"
             << coordinator_.ToStringStatus();
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  int i = 0;
  for (const auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);
    tmp_stream << "  slave[" << i << "]: " << slave_ptr->ToString() << "\r\n" << slave_ptr->ToStringStatus();
    i++;
  }
  return tmp_stream.str();
}

void SyncMasterSlot::GetValidSlaveNames(std::vector<std::string>* slavenames) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  for (const auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);
    if (slave_ptr->slave_state != kSlaveBinlogSync) {
      continue;
    }
    std::string name = slave_ptr->Ip() + ":" + std::to_string(slave_ptr->Port());
    slavenames->push_back(name);
  }
}

Status SyncMasterSlot::GetInfo(std::string* info) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  std::stringstream tmp_stream;
  tmp_stream << "  Role: Master"
             << "\r\n";
  tmp_stream << "  connected_slaves: " << slaves.size() << "\r\n";
  int i = 0;
  for (const auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    std::lock_guard l(slave_ptr->slave_mu);
    tmp_stream << "  slave[" << i++ << "]: " << slave_ptr->Ip() << ":" << std::to_string(slave_ptr->Port()) << "\r\n";
    tmp_stream << "  replication_status: " << SlaveStateMsg[slave_ptr->slave_state] << "\r\n";
    if (slave_ptr->slave_state == kSlaveBinlogSync) {
      BinlogOffset binlog_offset;
      Status s = Logger()->GetProducerStatus(&(binlog_offset.filenum), &(binlog_offset.offset));
      if (!s.ok()) {
        return s;
      }
      uint64_t lag =
          (binlog_offset.filenum - slave_ptr->acked_offset.b_offset.filenum) * g_pika_conf->binlog_file_size() +
          (binlog_offset.offset - slave_ptr->acked_offset.b_offset.offset);
      tmp_stream << "  lag: " << lag << "\r\n";
    }
  }
  info->append(tmp_stream.str());
  return Status::OK();
}

int32_t SyncMasterSlot::GenSessionId() {
  std::lock_guard ml(session_mu_);
  return session_id_++;
}

bool SyncMasterSlot::CheckSessionId(const std::string& ip, int port, const std::string& db_name,
                                         uint64_t slot_id, int session_id) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    LOG(WARNING) << "Check SessionId Get Slave Node Error: " << ip << ":" << port << "," << db_name << "_"
                 << slot_id;
    return false;
  }

  std::lock_guard l(slave_ptr->slave_mu);
  if (session_id != slave_ptr->SessionId()) {
    LOG(WARNING) << "Check SessionId Mismatch: " << ip << ":" << port << ", " << db_name << "_" << slot_id
                 << " expected_session: " << session_id << ", actual_session:" << slave_ptr->SessionId();
    return false;
  }
  return true;
}

Status SyncMasterSlot::ConsensusProposeLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  return coordinator_.ProposeLog(cmd_ptr);
}

Status SyncMasterSlot::ConsensusProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute) {
  return coordinator_.ProcessLeaderLog(cmd_ptr, attribute);
}

LogOffset SyncMasterSlot::ConsensusCommittedIndex() { return coordinator_.committed_index(); }

LogOffset SyncMasterSlot::ConsensusLastIndex() { return coordinator_.MemLogger()->last_offset(); }

uint32_t SyncMasterSlot::ConsensusTerm() { return coordinator_.term(); }

void SyncMasterSlot::ConsensusUpdateTerm(uint32_t term) {
  coordinator_.UpdateTerm(term);
  if ((g_pika_server->role() & PIKA_ROLE_MASTER) != 0) {
    CommitPreviousLogs(term);
  }
}

void SyncMasterSlot::CommitPreviousLogs(const uint32_t& term) {
  // Append dummy cmd
  std::shared_ptr<Cmd> dummy_ptr = std::make_shared<DummyCmd>(kCmdDummy, 0, kCmdFlagsWrite | kCmdFlagsSingleSlot);
  PikaCmdArgsType args;
  args.push_back(kCmdDummy);
  dummy_ptr->Initial(args, SyncSlotInfo().db_name_);
  dummy_ptr->SetStage(Cmd::kBinlogStage);
  dummy_ptr->Execute();
  dummy_ptr->SetStage(Cmd::kExecuteStage);
}

std::shared_ptr<SlaveNode> SyncMasterSlot::GetSlaveNode(const std::string& ip, int port) {
  return coordinator_.SyncPros().GetSlaveNode(ip, port);
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncMasterSlot::GetAllSlaveNodes() {
  return coordinator_.SyncPros().GetAllSlaveNodes();
}

Status SyncMasterSlot::ConsensusLeaderNegotiate(const LogOffset& f_last_offset, bool* reject,
                                                     std::vector<LogOffset>* hints) {
  return coordinator_.LeaderNegotiate(f_last_offset, reject, hints);
}

Status SyncMasterSlot::ConsensusFollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset) {
  return coordinator_.FollowerNegotiate(hints, reply_offset);
}

Status SyncMasterSlot::ConsensusReset(const LogOffset& applied_offset) { return coordinator_.Reset(applied_offset); }

/* SyncSlaveSlot */
SyncSlaveSlot::SyncSlaveSlot(const std::string& db_name, uint32_t slot_id)
    : SyncSlot(db_name, slot_id) {
  std::string dbsync_path = g_pika_conf->db_sync_path() + "/" + db_name;
  rsync_cli_.reset(new rsync::RsyncClient(dbsync_path, db_name, slot_id));
  m_info_.SetLastRecvTime(pstd::NowMicros());
}

void SyncSlaveSlot::SetReplState(const ReplState& repl_state) {
  if (repl_state == ReplState::kNoConnect) {
    // deactivate
    Deactivate();
    return;
  }
  std::lock_guard l(slot_mu_);
  repl_state_ = repl_state;
}

ReplState SyncSlaveSlot::State() {
  std::lock_guard l(slot_mu_);
  return repl_state_;
}

void SyncSlaveSlot::SetLastRecvTime(uint64_t time) {
  std::lock_guard l(slot_mu_);
  m_info_.SetLastRecvTime(time);
}

uint64_t SyncSlaveSlot::LastRecvTime() {
  std::lock_guard l(slot_mu_);
  return m_info_.LastRecvTime();
}

Status SyncSlaveSlot::CheckSyncTimeout(uint64_t now) {
  std::lock_guard l(slot_mu_);
  // no need to do session keepalive return ok
  if (repl_state_ != ReplState::kWaitDBSync && repl_state_ != ReplState::kConnected) {
    return Status::OK();
  }
  if (m_info_.LastRecvTime() + kRecvKeepAliveTimeout < now) {
    // update slave state to kTryConnect, and try reconnect to master node
    repl_state_ = ReplState::kTryConnect;
    g_pika_server->SetLoopSlotStateMachine(true);
  }
  return Status::OK();
}

Status SyncSlaveSlot::GetInfo(std::string* info) {
  std::string tmp_str = "  Role: Slave\r\n";
  tmp_str += "  master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n";
  tmp_str += "  slave status: " + ReplStateMsg[repl_state_] + "\r\n";
  info->append(tmp_str);
  return Status::OK();
}

void SyncSlaveSlot::Activate(const RmNode& master, const ReplState& repl_state) {
  std::lock_guard l(slot_mu_);
  m_info_ = master;
  repl_state_ = repl_state;
  m_info_.SetLastRecvTime(pstd::NowMicros());
}

void SyncSlaveSlot::Deactivate() {
  std::lock_guard l(slot_mu_);
  m_info_ = RmNode();
  repl_state_ = ReplState::kNoConnect;
  rsync_cli_->Stop();
}

std::string SyncSlaveSlot::ToStringStatus() {
  return "  Master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n" +
         "  SessionId: " + std::to_string(MasterSessionId()) + "\r\n" + "  SyncStatus " + ReplStateMsg[repl_state_] +
         "\r\n";
}

const std::string& SyncSlaveSlot::MasterIp() {
  std::lock_guard l(slot_mu_);
  return m_info_.Ip();
}

int SyncSlaveSlot::MasterPort() {
  std::lock_guard l(slot_mu_);
  return m_info_.Port();
}

void SyncSlaveSlot::SetMasterSessionId(int32_t session_id) {
  std::lock_guard l(slot_mu_);
  m_info_.SetSessionId(session_id);
}

int32_t SyncSlaveSlot::MasterSessionId() {
  std::lock_guard l(slot_mu_);
  return m_info_.SessionId();
}

void SyncSlaveSlot::SetLocalIp(const std::string& local_ip) {
  std::lock_guard l(slot_mu_);
  local_ip_ = local_ip;
}

std::string SyncSlaveSlot::LocalIp() {
  std::lock_guard l(slot_mu_);
  return local_ip_;
}

void SyncSlaveSlot::StopRsync() {
  rsync_cli_->Stop();
}

void SyncSlaveSlot::ActivateRsync() {
  if (!rsync_cli_->IsIdle()) {
    return;
  }
  LOG(WARNING) << "ActivateRsync ...";
  if (rsync_cli_->Init()) {
    rsync_cli_->Start();
  } else {
    SetReplState(ReplState::kError);
  }
}

/* PikaReplicaManger */

PikaReplicaManager::PikaReplicaManager() {
  std::set<std::string> ips;
  ips.insert("0.0.0.0");
  int port = g_pika_conf->port() + kPortShiftReplServer;
  pika_repl_client_ = std::make_unique<PikaReplClient>(3000, 60);
  pika_repl_server_ = std::make_unique<PikaReplServer>(ips, port, 3000);
  InitSlot();
}

void PikaReplicaManager::Start() {
  int ret = 0;
  ret = pika_repl_client_->Start();
  if (ret != net::kSuccess) {
    LOG(FATAL) << "Start Repl Client Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }

  ret = pika_repl_server_->Start();
  if (ret != net::kSuccess) {
    LOG(FATAL) << "Start Repl Server Error: " << ret
               << (ret == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
}

void PikaReplicaManager::Stop() {
  pika_repl_client_->Stop();
  pika_repl_server_->Stop();
}

bool PikaReplicaManager::CheckMasterSyncFinished() {
  for (auto& iter : sync_master_slots_) {
    std::shared_ptr<SyncMasterSlot> slot = iter.second;
    LogOffset commit = slot->ConsensusCommittedIndex();
    BinlogOffset binlog;
    Status s = slot->StableLogger()->Logger()->GetProducerStatus(&binlog.filenum, &binlog.offset);
    if (!s.ok()) {
      return false;
    }
    if (commit.b_offset < binlog) {
      return false;
    }
  }
  return true;
}

void PikaReplicaManager::InitSlot() {
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
  for (const auto& db : db_structs) {
    const std::string& db_name = db.db_name;
    for (const auto& slot_id : db.slot_ids) {
      sync_master_slots_[SlotInfo(db_name, slot_id)] =
          std::make_shared<SyncMasterSlot>(db_name, slot_id);
      sync_slave_slots_[SlotInfo(db_name, slot_id)] =
          std::make_shared<SyncSlaveSlot>(db_name, slot_id);
    }
  }
}

void PikaReplicaManager::ProduceWriteQueue(const std::string& ip, int port, uint32_t slot_id,
                                           const std::vector<WriteTask>& tasks) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  for (auto& task : tasks) {
    write_queues_[index][slot_id].push(task);
  }
}

int PikaReplicaManager::ConsumeWriteQueue() {
  std::unordered_map<std::string, std::vector<std::vector<WriteTask>>> to_send_map;
  int counter = 0;
  {
    std::lock_guard l(write_queue_mu_);
    for (auto& iter : write_queues_) {
      const std::string& ip_port = iter.first;
      std::unordered_map<uint32_t, std::queue<WriteTask>>& p_map = iter.second;
      for (auto& slot_queue : p_map) {
        std::queue<WriteTask>& queue = slot_queue.second;
        for (int i = 0; i < kBinlogSendPacketNum; ++i) {
          if (queue.empty()) {
            break;
          }
          size_t batch_index = queue.size() > kBinlogSendBatchNum ? kBinlogSendBatchNum : queue.size();
          std::vector<WriteTask> to_send;
          size_t batch_size = 0;
          for (size_t i = 0; i < batch_index; ++i) {
            WriteTask& task = queue.front();
            batch_size += task.binlog_chip_.binlog_.size();
            // make sure SerializeToString will not over 2G
            if (batch_size > PIKA_MAX_CONN_RBUF_HB) {
              break;
            }
            to_send.push_back(task);
            queue.pop();
            counter++;
          }
          if (!to_send.empty()) {
            to_send_map[ip_port].push_back(std::move(to_send));
          }
        }
      }
    }
  }

  std::vector<std::string> to_delete;
  for (auto& iter : to_send_map) {
    std::string ip;
    int port = 0;
    if (!pstd::ParseIpPortString(iter.first, ip, port)) {
      LOG(WARNING) << "Parse ip_port error " << iter.first;
      continue;
    }
    for (auto& to_send : iter.second) {
      Status s = pika_repl_server_->SendSlaveBinlogChips(ip, port, to_send);
      if (!s.ok()) {
        LOG(WARNING) << "send binlog to " << ip << ":" << port << " failed, " << s.ToString();
        to_delete.push_back(iter.first);
        continue;
      }
    }
  }

  if (!to_delete.empty()) {
    {
      std::lock_guard l(write_queue_mu_);
      for (auto& del_queue : to_delete) {
        write_queues_.erase(del_queue);
      }
    }
  }
  return counter;
}

void PikaReplicaManager::DropItemInWriteQueue(const std::string& ip, int port) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  write_queues_.erase(index);
}

void PikaReplicaManager::ScheduleReplServerBGTask(net::TaskFunc func, void* arg) {
  pika_repl_server_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleReplClientBGTask(net::TaskFunc func, void* arg) {
  pika_repl_client_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleWriteBinlogTask(const std::string& db_slot,
                                                 const std::shared_ptr<InnerMessage::InnerResponse>& res,
                                                 const std::shared_ptr<net::PbConn>& conn, void* res_private_data) {
  pika_repl_client_->ScheduleWriteBinlogTask(db_slot, res, conn, res_private_data);
}

void PikaReplicaManager::ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const LogOffset& offset,
                                             const std::string& db_name, uint32_t slot_id) {
  pika_repl_client_->ScheduleWriteDBTask(cmd_ptr, offset, db_name, slot_id);
}

void PikaReplicaManager::ReplServerRemoveClientConn(int fd) { pika_repl_server_->RemoveClientConn(fd); }

void PikaReplicaManager::ReplServerUpdateClientConnMap(const std::string& ip_port, int fd) {
  pika_repl_server_->UpdateClientConnMap(ip_port, fd);
}

Status PikaReplicaManager::UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start,
                                                  const LogOffset& offset_end) {
  std::shared_lock l(slots_rw_);
  if (sync_master_slots_.find(slave.NodeSlotInfo()) == sync_master_slots_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterSlot> slot = sync_master_slots_[slave.NodeSlotInfo()];
  Status s = slot->ConsensusUpdateSlave(slave.Ip(), slave.Port(), offset_start, offset_end);
  if (!s.ok()) {
    return s;
  }
  s = slot->SyncBinlogToWq(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

bool PikaReplicaManager::CheckSlaveSlotState(const std::string& ip, const int port) {
  std::shared_ptr<SyncSlaveSlot> slot = nullptr;
  for (const auto& iter : g_pika_rm->sync_slave_slots_) {
    slot = iter.second;
    if (slot->State() == ReplState::kDBNoConnect && slot->MasterIp() == ip &&
        slot->MasterPort() + kPortShiftReplServer == port) {
      LOG(INFO) << "DB: " << slot->SyncSlotInfo().ToString()
                << " has been dbslaveof no one, then will not try reconnect.";
      return false;
    }
  }
  return true;
}

Status PikaReplicaManager::LostConnection(const std::string& ip, int port) {
  std::shared_lock l(slots_rw_);
  for (auto& iter : sync_master_slots_) {
    std::shared_ptr<SyncMasterSlot> slot = iter.second;
    Status s = slot->RemoveSlaveNode(ip, port);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(WARNING) << "Lost Connection failed " << s.ToString();
    }
  }

  for (auto& iter : sync_slave_slots_) {
    std::shared_ptr<SyncSlaveSlot> slot = iter.second;
    if (slot->MasterIp() == ip && slot->MasterPort() == port) {
      slot->Deactivate();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::WakeUpBinlogSync() {
  std::shared_lock l(slots_rw_);
  for (auto& iter : sync_master_slots_) {
    std::shared_ptr<SyncMasterSlot> slot = iter.second;
    Status s = slot->WakeUpSlaveBinlogSync();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckSyncTimeout(uint64_t now) {
  std::shared_lock l(slots_rw_);

  for (auto& iter : sync_master_slots_) {
    std::shared_ptr<SyncMasterSlot> slot = iter.second;
    Status s = slot->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  for (auto& iter : sync_slave_slots_) {
    std::shared_ptr<SyncSlaveSlot> slot = iter.second;
    Status s = slot->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckSlotRole(const std::string& db, uint32_t slot_id, int* role) {
  std::shared_lock l(slots_rw_);
  *role = 0;
  SlotInfo p_info(db, slot_id);
  if (sync_master_slots_.find(p_info) == sync_master_slots_.end()) {
    return Status::NotFound(db + std::to_string(slot_id) + " not found");
  }
  if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
    return Status::NotFound(db + std::to_string(slot_id) + " not found");
  }
  if (sync_master_slots_[p_info]->GetNumberOfSlaveNode() != 0 ||
      (sync_master_slots_[p_info]->GetNumberOfSlaveNode() == 0 &&
       sync_slave_slots_[p_info]->State() == kNoConnect)) {
    *role |= PIKA_ROLE_MASTER;
  }
  if (sync_slave_slots_[p_info]->State() != ReplState::kNoConnect) {
    *role |= PIKA_ROLE_SLAVE;
  }
  // if role is not master or slave, the rest situations are all single
  return Status::OK();
}

Status PikaReplicaManager::GetSlotInfo(const std::string& db, uint32_t slot_id, std::string* info) {
  int role = 0;
  std::string tmp_res;
  Status s = CheckSlotRole(db, slot_id, &role);
  if (!s.ok()) {
    return s;
  }

  bool add_divider_line = (((role & PIKA_ROLE_MASTER) != 0) && ((role & PIKA_ROLE_SLAVE) != 0));
  std::shared_lock l(slots_rw_);
  SlotInfo p_info(db, slot_id);
  if ((role & PIKA_ROLE_MASTER) != 0) {
    if (sync_master_slots_.find(p_info) == sync_master_slots_.end()) {
      return Status::NotFound(db + std::to_string(slot_id) + " not found");
    }
    Status s = sync_master_slots_[p_info]->GetInfo(info);
    if (!s.ok()) {
      return s;
    }
  }
  if (add_divider_line) {
    info->append("  -----------\r\n");
  }
  if ((role & PIKA_ROLE_SLAVE) != 0) {
    if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
      return Status::NotFound(db + std::to_string(slot_id) + " not found");
    }
    Status s = sync_slave_slots_[p_info]->GetInfo(info);
    if (!s.ok()) {
      return s;
    }
  }
  info->append("\r\n");
  return Status::OK();
}

Status PikaReplicaManager::SelectLocalIp(const std::string& remote_ip, const int remote_port,
                                         std::string* const local_ip) {
  std::unique_ptr<net::NetCli> cli(net::NewRedisCli());
  cli->set_connect_timeout(1500);
  if ((cli->Connect(remote_ip, remote_port, "")).ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    std::string tmp_ip(inet_ntoa(laddr.sin_addr));
    *local_ip = tmp_ip;
    cli->Close();
  } else {
    LOG(WARNING) << "Failed to connect remote node(" << remote_ip << ":" << remote_port << ")";
    return Status::Corruption("connect remote node error");
  }
  return Status::OK();
}

Status PikaReplicaManager::ActivateSyncSlaveSlot(const RmNode& node, const ReplState& repl_state) {
  std::shared_lock l(slots_rw_);
  const SlotInfo& p_info = node.NodeSlotInfo();
  if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
    return Status::NotFound("Sync Slave Slot " + node.ToString() + " not found");
  }
  ReplState ssp_state = sync_slave_slots_[p_info]->State();
  if (ssp_state != ReplState::kNoConnect && ssp_state != ReplState::kDBNoConnect) {
    return Status::Corruption("Sync Slave Slot in " + ReplStateMsg[ssp_state]);
  }
  std::string local_ip;
  Status s = SelectLocalIp(node.Ip(), node.Port(), &local_ip);
  if (s.ok()) {
    sync_slave_slots_[p_info]->SetLocalIp(local_ip);
    sync_slave_slots_[p_info]->Activate(node, repl_state);
  }
  return s;
}

Status PikaReplicaManager::DeactivateSyncSlaveSlot(const SlotInfo& p_info) {
  std::shared_lock l(slots_rw_);
  if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
    return Status::NotFound("Sync Slave Slot " + p_info.ToString());
  }
  sync_slave_slots_[p_info]->Deactivate();
  return Status::OK();
}

Status PikaReplicaManager::SendMetaSyncRequest() {
  Status s;
  if (time(nullptr) - g_pika_server->GetMetaSyncTimestamp() >= PIKA_META_SYNC_MAX_WAIT_TIME ||
      g_pika_server->IsFirstMetaSync()) {
    s = pika_repl_client_->SendMetaSync();
    if (s.ok()) {
      g_pika_server->UpdateMetaSyncTimestamp();
      g_pika_server->SetFirstMetaSync(false);
    }
  }
  return s;
}

Status PikaReplicaManager::SendRemoveSlaveNodeRequest(const std::string& db, uint32_t slot_id) {
  pstd::Status s;
  std::shared_lock l(slots_rw_);
  SlotInfo p_info(db, slot_id);
  if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
    return Status::NotFound("Sync Slave Slot " + p_info.ToString());
  } else {
    std::shared_ptr<SyncSlaveSlot> s_slot = sync_slave_slots_[p_info];
    s = pika_repl_client_->SendRemoveSlaveNode(s_slot->MasterIp(), s_slot->MasterPort(), db, slot_id,
                                               s_slot->LocalIp());
    if (s.ok()) {
      s_slot->SetReplState(ReplState::kDBNoConnect);
    }
  }

  if (s.ok()) {
    LOG(INFO) << "SlaveNode (" << db << ":" << slot_id << "), stop sync success";
  } else {
    LOG(WARNING) << "SlaveNode (" << db << ":" << slot_id << "), stop sync faild, " << s.ToString();
  }
  return s;
}

Status PikaReplicaManager::SendSlotTrySyncRequest(const std::string& db_name, size_t slot_id) {
  BinlogOffset boffset;
  if (!g_pika_server->GetDBSlotBinlogOffset(db_name, slot_id, &boffset)) {
    LOG(WARNING) << "Slot: " << db_name << ":" << slot_id << ",  Get Slot binlog offset failed";
    return Status::Corruption("Slot get binlog offset error");
  }

  std::shared_ptr<SyncSlaveSlot> slave_slot =
      GetSyncSlaveSlotByName(SlotInfo(db_name, slot_id));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << db_name << ":" << slot_id << ", NotFound";
    return Status::Corruption("Slave Slot not found");
  }

  Status status =
      pika_repl_client_->SendSlotTrySync(slave_slot->MasterIp(), slave_slot->MasterPort(), db_name,
                                              slot_id, boffset, slave_slot->LocalIp());

  if (status.ok()) {
    slave_slot->SetReplState(ReplState::kWaitReply);
  } else {
    slave_slot->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendSlotTrySyncRequest failed " << status.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendSlotDBSyncRequest(const std::string& db_name, size_t slot_id) {
  BinlogOffset boffset;
  if (!g_pika_server->GetDBSlotBinlogOffset(db_name, slot_id, &boffset)) {
    LOG(WARNING) << "Slot: " << db_name << ":" << slot_id << ",  Get slot binlog offset failed";
    return Status::Corruption("Slot get binlog offset error");
  }

  std::shared_ptr<Slot> slot = g_pika_server->GetDBSlotById(db_name, slot_id);
  if (!slot) {
    LOG(WARNING) << "Slot: " << db_name << ":" << slot_id << ", NotFound";
    return Status::Corruption("Slot not found");
  }
  slot->PrepareRsync();

  std::shared_ptr<SyncSlaveSlot> slave_slot =
      GetSyncSlaveSlotByName(SlotInfo(db_name, slot_id));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << db_name << ":" << slot_id << ", NotFound";
    return Status::Corruption("Slave Slot not found");
  }

  Status status = pika_repl_client_->SendSlotDBSync(slave_slot->MasterIp(), slave_slot->MasterPort(),
                                                    db_name, slot_id, boffset, slave_slot->LocalIp());

  Status s;
  if (status.ok()) {
    slave_slot->SetReplState(ReplState::kWaitReply);
  } else {
    slave_slot->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendSlotDBSync failed " << status.ToString();
  }
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendSlotBinlogSyncAckRequest(const std::string& db, uint32_t slot_id,
                                                             const LogOffset& ack_start, const LogOffset& ack_end,
                                                             bool is_first_send) {
  std::shared_ptr<SyncSlaveSlot> slave_slot = GetSyncSlaveSlotByName(SlotInfo(db, slot_id));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << db << ":" << slot_id << ", NotFound";
    return Status::Corruption("Slave Slot not found");
  }
  return pika_repl_client_->SendSlotBinlogSync(slave_slot->MasterIp(), slave_slot->MasterPort(), db,
                                                    slot_id, ack_start, ack_end, slave_slot->LocalIp(),
                                                    is_first_send);
}

Status PikaReplicaManager::CloseReplClientConn(const std::string& ip, int32_t port) {
  return pika_repl_client_->Close(ip, port);
}

Status PikaReplicaManager::SendSlaveBinlogChipsRequest(const std::string& ip, int port,
                                                       const std::vector<WriteTask>& tasks) {
  return pika_repl_server_->SendSlaveBinlogChips(ip, port, tasks);
}

std::shared_ptr<SyncMasterSlot> PikaReplicaManager::GetSyncMasterSlotByName(const SlotInfo& p_info) {
  std::shared_lock l(slots_rw_);
  if (sync_master_slots_.find(p_info) == sync_master_slots_.end()) {
    return nullptr;
  }
  return sync_master_slots_[p_info];
}

std::shared_ptr<SyncSlaveSlot> PikaReplicaManager::GetSyncSlaveSlotByName(const SlotInfo& p_info) {
  std::shared_lock l(slots_rw_);
  if (sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
    return nullptr;
  }
  return sync_slave_slots_[p_info];
}

Status PikaReplicaManager::RunSyncSlaveSlotStateMachine() {
  std::shared_lock l(slots_rw_);
  for (const auto& item : sync_slave_slots_) {
    SlotInfo p_info = item.first;
    std::shared_ptr<SyncSlaveSlot> s_slot = item.second;
    if (s_slot->State() == ReplState::kTryConnect) {
      SendSlotTrySyncRequest(p_info.db_name_, p_info.slot_id_);
    } else if (s_slot->State() == ReplState::kTryDBSync) {
      SendSlotDBSyncRequest(p_info.db_name_, p_info.slot_id_);
    } else if (s_slot->State() == ReplState::kWaitReply) {
      continue;
    } else if (s_slot->State() == ReplState::kWaitDBSync) {
      s_slot->ActivateRsync();
      std::shared_ptr<Slot> slot =
          g_pika_server->GetDBSlotById(p_info.db_name_, p_info.slot_id_);
      if (slot) {
        if (!s_slot->IsRsyncRunning()) {
          slot->TryUpdateMasterOffset();
        }
      } else {
        LOG(WARNING) << "Slot not found, DB Name: " << p_info.db_name_
                     << " Slot Id: " << p_info.slot_id_;
      }
    } else if (s_slot->State() == ReplState::kConnected || s_slot->State() == ReplState::kNoConnect ||
               s_slot->State() == ReplState::kDBNoConnect) {
      continue;
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSyncSlotSanityCheck(const std::set<SlotInfo>& p_infos) {
  std::shared_lock l(slots_rw_);
  for (const auto& p_info : p_infos) {
    if (sync_master_slots_.find(p_info) != sync_master_slots_.end() ||
        sync_slave_slots_.find(p_info) != sync_slave_slots_.end()) {
      LOG(WARNING) << "sync slot: " << p_info.ToString() << " exist";
      return Status::Corruption("sync slot " + p_info.ToString() + " exist");
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSyncSlot(const std::set<SlotInfo>& p_infos) {
  Status s = AddSyncSlotSanityCheck(p_infos);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(slots_rw_);
  for (const auto& p_info : p_infos) {
    sync_master_slots_[p_info] = std::make_shared<SyncMasterSlot>(p_info.db_name_, p_info.slot_id_);
    sync_slave_slots_[p_info] = std::make_shared<SyncSlaveSlot>(p_info.db_name_, p_info.slot_id_);
  }
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncSlotSanityCheck(const std::set<SlotInfo>& p_infos) {
  std::shared_lock l(slots_rw_);
  for (const auto& p_info : p_infos) {
    if (sync_master_slots_.find(p_info) == sync_master_slots_.end() ||
        sync_slave_slots_.find(p_info) == sync_slave_slots_.end()) {
      LOG(WARNING) << "sync slot: " << p_info.ToString() << " not found";
      return Status::Corruption("sync slot " + p_info.ToString() + " not found");
    }

    if (sync_master_slots_[p_info]->GetNumberOfSlaveNode() != 0) {
      LOG(WARNING) << "sync Master Slot: " << p_info.ToString() << " in syncing";
      return Status::Corruption("sync Master Slot " + p_info.ToString() + " in syncing");
    }

    ReplState state = sync_slave_slots_[p_info]->State();
    if (state != kNoConnect && state != kError) {
      LOG(WARNING) << "sync Slave Slot: " << p_info.ToString() << " in " << ReplStateMsg[state] + " state";
      return Status::Corruption("sync Slave Slot " + p_info.ToString() + " in " + ReplStateMsg[state] + " state");
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncSlot(const std::set<SlotInfo>& p_infos) {
  Status s = RemoveSyncSlotSanityCheck(p_infos);
  if (!s.ok()) {
    return s;
  }

  std::lock_guard l(slots_rw_);
  for (const auto& p_info : p_infos) {
    if (sync_master_slots_.find(p_info) != sync_master_slots_.end()) {
      sync_master_slots_[p_info]->StableLogger()->Leave();
    }
    sync_master_slots_.erase(p_info);
    sync_slave_slots_.erase(p_info);
  }
  return Status::OK();
}

Status PikaReplicaManager::SyncDBSanityCheck(const std::string& db_name) {
  std::shared_lock l(slots_rw_);
  for (const auto& master_slot : sync_master_slots_) {
    if (master_slot.first.db_name_ == db_name) {
      LOG(WARNING) << "sync slot: " << master_slot.first.ToString() << " exist";
      return Status::Corruption("sync slot " + master_slot.first.ToString() + " exist");
    }
  }
  for (const auto& slave_slot : sync_slave_slots_) {
    if (slave_slot.first.db_name_ == db_name) {
      LOG(WARNING) << "sync slot: " << slave_slot.first.ToString() << " exist";
      return Status::Corruption("sync slot " + slave_slot.first.ToString() + " exist");
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::DelSyncDB(const std::string& db_name) {
  Status s = SyncDBSanityCheck(db_name);
  if (!s.ok()) {
    return s;
  }
  std::string db_log_path = g_pika_conf->log_path() + "log_" + db_name;
  std::string db_log_path_tmp = db_log_path + "_deleting/";
  if (pstd::RenameFile(db_log_path, db_log_path_tmp) != 0) {
    LOG(WARNING) << "Failed to move log to trash, error: " << strerror(errno);
    return Status::Corruption("Failed to move log to trash");
  }
  g_pika_server->PurgeDir(db_log_path_tmp);
  LOG(WARNING) << "Slot StableLog: " << db_name << " move to trash success";
  return Status::OK();
}

void PikaReplicaManager::FindCompleteReplica(std::vector<std::string>* replica) {
  std::unordered_map<std::string, size_t> replica_slotnum;
  std::shared_lock l(slots_rw_);
  for (auto& iter : sync_master_slots_) {
    std::vector<std::string> names;
    iter.second->GetValidSlaveNames(&names);
    for (auto& name : names) {
      if (replica_slotnum.find(name) == replica_slotnum.end()) {
        replica_slotnum[name] = 0;
      }
      replica_slotnum[name]++;
    }
  }
  for (const auto& item : replica_slotnum) {
    if (item.second == sync_master_slots_.size()) {
      replica->push_back(item.first);
    }
  }
}

void PikaReplicaManager::FindCommonMaster(std::string* master) {
  std::shared_lock l(slots_rw_);
  std::string common_master_ip;
  int common_master_port = 0;
  for (auto& iter : sync_slave_slots_) {
    if (iter.second->State() != kConnected) {
      return;
    }
    std::string tmp_ip = iter.second->MasterIp();
    int tmp_port = iter.second->MasterPort();
    if (common_master_ip.empty() && common_master_port == 0) {
      common_master_ip = tmp_ip;
      common_master_port = tmp_port;
    }
    if (tmp_ip != common_master_ip || tmp_port != common_master_port) {
      return;
    }
  }
  if (!common_master_ip.empty() && common_master_port != 0) {
    *master = common_master_ip + ":" + std::to_string(common_master_port);
  }
}

void PikaReplicaManager::RmStatus(std::string* info) {
  std::shared_lock l(slots_rw_);
  std::stringstream tmp_stream;
  tmp_stream << "Master Slot(" << sync_master_slots_.size() << "):"
             << "\r\n";
  for (auto& iter : sync_master_slots_) {
    tmp_stream << " Slot " << iter.second->SyncSlotInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";
  }
  tmp_stream << "Slave Slot(" << sync_slave_slots_.size() << "):"
             << "\r\n";
  for (auto& iter : sync_slave_slots_) {
    tmp_stream << " Slot " << iter.second->SyncSlotInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";
  }
  info->append(tmp_stream.str());
}
