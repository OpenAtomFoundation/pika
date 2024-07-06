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

/* SyncDB */

SyncDB::SyncDB(const std::string& db_name)
    : db_info_(db_name) {}

std::string SyncDB::DBName() {
  return db_info_.db_name_;
}

/* SyncMasterDB*/

SyncMasterDB::SyncMasterDB(const std::string& db_name)
    : SyncDB(db_name),  coordinator_(db_name) {}

int SyncMasterDB::GetNumberOfSlaveNode() { return coordinator_.SyncPros().SlaveSize(); }

bool SyncMasterDB::CheckSlaveNodeExist(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  return static_cast<bool>(slave_ptr);
}

Status SyncMasterDB::GetSlaveNodeSession(const std::string& ip, int port, int32_t* session) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("slave " + ip + ":" + std::to_string(port) + " not found");
  }

  slave_ptr->Lock();
  *session = slave_ptr->SessionId();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterDB::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = coordinator_.AddSlaveNode(ip, port, session_id);
  if (!s.ok()) {
    LOG(WARNING) << "Add Slave Node Failed, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":"
                 << port;
    return s;
  }
  LOG(INFO) << "Add Slave Node, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();
}

Status SyncMasterDB::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = coordinator_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    LOG(WARNING) << "Remove Slave Node Failed, db: " << SyncDBInfo().ToString() << ", ip_port: " << ip
                 << ":" << port;
    return s;
  }
  LOG(INFO) << "Remove Slave Node, DB: " << SyncDBInfo().ToString() << ", ip_port: " << ip << ":" << port;
  return Status::OK();
}

Status SyncMasterDB::ActivateSlaveBinlogSync(const std::string& ip, int port, const LogOffset& offset) {
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
    g_pika_rm->DropItemInOneWriteQueue(ip, port, slave_ptr->DBName());
    slave_ptr->sync_win.Reset();
    slave_ptr->b_state = kReadFromFile;
  }

  Status s = SyncBinlogToWq(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status SyncMasterDB::SyncBinlogToWq(const std::string& ip, int port) {
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

Status SyncMasterDB::ActivateSlaveDbSync(const std::string& ip, int port) {
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

Status SyncMasterDB::ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
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
      LOG(WARNING) << SyncDBInfo().ToString() << " Read Binlog error : " << s.ToString();
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
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SessionId());
    WriteTask task(rm_node, BinlogChip(sent_offset, msg), slave_ptr->sent_offset);
    tasks.push_back(task);
    slave_ptr->sent_offset = sent_offset;
  }

  if (!tasks.empty()) {
    g_pika_rm->ProduceWriteQueue(slave_ptr->Ip(), slave_ptr->Port(), db_info_.db_name_, tasks);
  }
  return Status::OK();
}

Status SyncMasterDB::ConsensusUpdateSlave(const std::string& ip, int port, const LogOffset& start, const LogOffset& end) {
  Status s = coordinator_.UpdateSlave(ip, port, start, end);
  if (!s.ok()) {
    LOG(WARNING) << SyncDBInfo().ToString() << s.ToString();
    return s;
  }
  return Status::OK();
}

Status SyncMasterDB::GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset,
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

Status SyncMasterDB::GetSlaveState(const std::string& ip, int port, SlaveState* const slave_state) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *slave_state = slave_ptr->slave_state;
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterDB::WakeUpSlaveBinlogSync() {
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

Status SyncMasterDB::SetLastRecvTime(const std::string& ip, int port, uint64_t time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->SetLastRecvTime(time);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterDB::GetSafetyPurgeBinlog(std::string* safety_purge) {
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

bool SyncMasterDB::BinlogCloudPurge(uint32_t index) {
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

Status SyncMasterDB::CheckSyncTimeout(uint64_t now) {
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
      RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->DBName(), slave_ptr->SessionId());
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
    g_pika_rm->DropItemInOneWriteQueue(node.Ip(), node.Port(), DBName());
    LOG(WARNING) << SyncDBInfo().ToString() << " Master del Recv Timeout slave success " << node.ToString();
  }
  return Status::OK();
}

std::string SyncMasterDB::ToStringStatus() {
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

int32_t SyncMasterDB::GenSessionId() {
  std::lock_guard ml(session_mu_);
  return session_id_++;
}

bool SyncMasterDB::CheckSessionId(const std::string& ip, int port, const std::string& db_name,
                                  int session_id) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    LOG(WARNING) << "Check SessionId Get Slave Node Error: " << ip << ":" << port << "," << db_name;
    return false;
  }

  std::lock_guard l(slave_ptr->slave_mu);
  if (session_id != slave_ptr->SessionId()) {
    LOG(WARNING) << "Check SessionId Mismatch: " << ip << ":" << port << ", " << db_name << "_"
                 << " expected_session: " << session_id << ", actual_session:" << slave_ptr->SessionId();
    return false;
  }
  return true;
}

Status SyncMasterDB::ConsensusProposeLog(const std::shared_ptr<Cmd>& cmd_ptr) {
  return coordinator_.ProposeLog(cmd_ptr);
}

Status SyncMasterDB::ConsensusProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute) {
  return coordinator_.ProcessLeaderLog(cmd_ptr, attribute);
}

LogOffset SyncMasterDB::ConsensusCommittedIndex() { return coordinator_.committed_index(); }

LogOffset SyncMasterDB::ConsensusLastIndex() { return coordinator_.MemLogger()->last_offset(); }

std::shared_ptr<SlaveNode> SyncMasterDB::GetSlaveNode(const std::string& ip, int port) {
  return coordinator_.SyncPros().GetSlaveNode(ip, port);
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncMasterDB::GetAllSlaveNodes() {
  return coordinator_.SyncPros().GetAllSlaveNodes();
}

/* SyncSlaveDB */
SyncSlaveDB::SyncSlaveDB(const std::string& db_name)
    : SyncDB(db_name) {
  std::string dbsync_path = g_pika_conf->db_sync_path() + "/" + db_name;
  rsync_cli_.reset(new rsync::RsyncClient(dbsync_path, db_name));
  m_info_.SetLastRecvTime(pstd::NowMicros());
}

void SyncSlaveDB::SetReplState(const ReplState& repl_state) {
  if (repl_state == ReplState::kNoConnect) {
    Deactivate();
    return;
  }
  std::lock_guard l(db_mu_);
  repl_state_ = repl_state;
}

ReplState SyncSlaveDB::State() {
  std::lock_guard l(db_mu_);
  return repl_state_;
}

void SyncSlaveDB::SetLastRecvTime(uint64_t time) {
  std::lock_guard l(db_mu_);
  m_info_.SetLastRecvTime(time);
}

Status SyncSlaveDB::CheckSyncTimeout(uint64_t now) {
  std::lock_guard l(db_mu_);
  // no need to do session keepalive return ok
  if (repl_state_ != ReplState::kWaitDBSync && repl_state_ != ReplState::kConnected) {
    return Status::OK();
  }
  if (m_info_.LastRecvTime() + kRecvKeepAliveTimeout < now) {
    // update slave state to kTryConnect, and try reconnect to master node
    repl_state_ = ReplState::kTryConnect;
  }
  return Status::OK();
}

Status SyncSlaveDB::GetInfo(std::string* info) {
  std::string tmp_str = "  Role: Slave\r\n";
  tmp_str += "  master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n";
  tmp_str += "  slave status: " + ReplStateMsg[repl_state_] + "\r\n";
  info->append(tmp_str);
  return Status::OK();
}

void SyncSlaveDB::Activate(const RmNode& master, const ReplState& repl_state) {
  std::lock_guard l(db_mu_);
  m_info_ = master;
  repl_state_ = repl_state;
  m_info_.SetLastRecvTime(pstd::NowMicros());
}

void SyncSlaveDB::Deactivate() {
  std::lock_guard l(db_mu_);
  m_info_ = RmNode();
  repl_state_ = ReplState::kNoConnect;
  rsync_cli_->Stop();
}

std::string SyncSlaveDB::ToStringStatus() {
  return "  Master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n" +
         "  SessionId: " + std::to_string(MasterSessionId()) + "\r\n" + "  SyncStatus " + ReplStateMsg[repl_state_] +
         "\r\n";
}

const std::string& SyncSlaveDB::MasterIp() {
  std::lock_guard l(db_mu_);
  return m_info_.Ip();
}

int SyncSlaveDB::MasterPort() {
  std::lock_guard l(db_mu_);
  return m_info_.Port();
}

void SyncSlaveDB::SetMasterSessionId(int32_t session_id) {
  std::lock_guard l(db_mu_);
  m_info_.SetSessionId(session_id);
}

int32_t SyncSlaveDB::MasterSessionId() {
  std::lock_guard l(db_mu_);
  return m_info_.SessionId();
}

void SyncSlaveDB::SetLocalIp(const std::string& local_ip) {
  std::lock_guard l(db_mu_);
  local_ip_ = local_ip;
}

std::string SyncSlaveDB::LocalIp() {
  std::lock_guard l(db_mu_);
  return local_ip_;
}

void SyncSlaveDB::StopRsync() {
  rsync_cli_->Stop();
}

pstd::Status SyncSlaveDB::ActivateRsync() {
  Status s = Status::OK();
  if (!rsync_cli_->IsIdle()) {
    return s;
  }
  LOG(WARNING) << "Slave DB: " << DBName() << " Activating Rsync ... (retry count:" << rsync_init_retry_count_ << ")";
  if (rsync_cli_->Init()) {
    rsync_init_retry_count_ = 0;
    rsync_cli_->Start();
    return s;
  } else {
    rsync_init_retry_count_ += 1;
    if (rsync_init_retry_count_ >= kMaxRsyncInitReTryTimes) {
      SetReplState(ReplState::kError);
      LOG(ERROR) << "Full Sync Stage - Rsync Init failed: Slave failed to pull meta info(generated by bgsave task in Master) from Master after MaxRsyncInitReTryTimes("
                 << kMaxRsyncInitReTryTimes << " times) is reached. This usually means the Master's bgsave task has costed an unexpected-long time.";
    }
    return Status::Error("rsync client init failed!");
  }
}

/* PikaReplicaManger */

PikaReplicaManager::PikaReplicaManager() {
  std::set<std::string> ips;
  ips.insert("0.0.0.0");
  int port = g_pika_conf->port() + kPortShiftReplServer;
  pika_repl_client_ = std::make_unique<PikaReplClient>(3000, 60);
  pika_repl_server_ = std::make_unique<PikaReplServer>(ips, port, 3000);
  InitDB();
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
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    LogOffset commit = db->ConsensusCommittedIndex();
    BinlogOffset binlog;
    Status s = db->StableLogger()->Logger()->GetProducerStatus(&binlog.filenum, &binlog.offset);
    if (!s.ok()) {
      return false;
    }
    if (commit.b_offset < binlog) {
      return false;
    }
  }
  return true;
}

void PikaReplicaManager::InitDB() {
  std::vector<DBStruct> db_structs = g_pika_conf->db_structs();
  for (const auto& db : db_structs) {
    const std::string& db_name = db.db_name;
    sync_master_dbs_[DBInfo(db_name)] = std::make_shared<SyncMasterDB>(db_name);
    sync_slave_dbs_[DBInfo(db_name)] = std::make_shared<SyncSlaveDB>(db_name);
  }
}

void PikaReplicaManager::ProduceWriteQueue(const std::string& ip, int port, std::string db_name,
                                           const std::vector<WriteTask>& tasks) {
  std::lock_guard l(write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  for (auto& task : tasks) {
    write_queues_[index][db_name].push(task);
  }
}

int PikaReplicaManager::ConsumeWriteQueue() {
  std::unordered_map<std::string, std::vector<std::vector<WriteTask>>> to_send_map;
  int counter = 0;
  {
    std::lock_guard l(write_queue_mu_);
    for (auto& iter : write_queues_) {
      const std::string& ip_port = iter.first;
      std::unordered_map<std::string, std::queue<WriteTask>>& p_map = iter.second;
      for (auto& db_queue : p_map) {
        std::queue<WriteTask>& queue = db_queue.second;
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
    std::lock_guard l(write_queue_mu_);
    for (auto& del_queue : to_delete) {
      write_queues_.erase(del_queue);
    }
  }
  return counter;
}

void PikaReplicaManager::DropItemInOneWriteQueue(const std::string& ip, int port, const std::string& db_name) {
    std::lock_guard l(write_queue_mu_);
    std::string index = ip + ":" + std::to_string(port);
    if (write_queues_.find(index) != write_queues_.end()) {
      write_queues_[index].erase(db_name);
    }
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

void PikaReplicaManager::ScheduleReplClientBGTaskByDBName(net::TaskFunc func, void* arg, const std::string &db_name) {
  pika_repl_client_->ScheduleByDBName(func, arg, db_name);
}

void PikaReplicaManager::ScheduleWriteBinlogTask(const std::string& db,
                                                 const std::shared_ptr<InnerMessage::InnerResponse>& res,
                                                 const std::shared_ptr<net::PbConn>& conn, void* res_private_data) {
  pika_repl_client_->ScheduleWriteBinlogTask(db, res, conn, res_private_data);
}

void PikaReplicaManager::ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const LogOffset& offset,
                                             const std::string& db_name) {
  pika_repl_client_->ScheduleWriteDBTask(cmd_ptr, offset, db_name);
}

void PikaReplicaManager::ReplServerRemoveClientConn(int fd) { pika_repl_server_->RemoveClientConn(fd); }

void PikaReplicaManager::ReplServerUpdateClientConnMap(const std::string& ip_port, int fd) {
  pika_repl_server_->UpdateClientConnMap(ip_port, fd);
}

Status PikaReplicaManager::UpdateSyncBinlogStatus(const RmNode& slave, const LogOffset& offset_start,
                                                  const LogOffset& offset_end) {
  std::shared_lock l(dbs_rw_);
  if (sync_master_dbs_.find(slave.NodeDBInfo()) == sync_master_dbs_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterDB> db = sync_master_dbs_[slave.NodeDBInfo()];
  Status s = db->ConsensusUpdateSlave(slave.Ip(), slave.Port(), offset_start, offset_end);
  if (!s.ok()) {
    return s;
  }
  s = db->SyncBinlogToWq(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

bool PikaReplicaManager::CheckSlaveDBState(const std::string& ip, const int port) {
  std::shared_ptr<SyncSlaveDB> db = nullptr;
  for (const auto& iter : g_pika_rm->sync_slave_dbs_) {
    db = iter.second;
    if (db->State() == ReplState::kDBNoConnect && db->MasterIp() == ip &&
        db->MasterPort() + kPortShiftReplServer == port) {
      LOG(INFO) << "DB: " << db->SyncDBInfo().ToString()
                << " has been dbslaveof no one, then will not try reconnect.";
      return false;
    }
  }
  return true;
}

Status PikaReplicaManager::LostConnection(const std::string& ip, int port) {
  std::shared_lock l(dbs_rw_);
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    Status s = db->RemoveSlaveNode(ip, port);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(WARNING) << "Lost Connection failed " << s.ToString();
    }
  }

  for (auto& iter : sync_slave_dbs_) {
    std::shared_ptr<SyncSlaveDB> db = iter.second;
    if (db->MasterIp() == ip && db->MasterPort() == port) {
      db->Deactivate();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::WakeUpBinlogSync() {
  std::shared_lock l(dbs_rw_);
  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    Status s = db->WakeUpSlaveBinlogSync();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckSyncTimeout(uint64_t now) {
  std::shared_lock l(dbs_rw_);

  for (auto& iter : sync_master_dbs_) {
    std::shared_ptr<SyncMasterDB> db = iter.second;
    Status s = db->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  for (auto& iter : sync_slave_dbs_) {
    std::shared_ptr<SyncSlaveDB> db = iter.second;
    Status s = db->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckDBRole(const std::string& db, int* role) {
  std::shared_lock l(dbs_rw_);
  *role = 0;
  DBInfo p_info(db);
  if (sync_master_dbs_.find(p_info) == sync_master_dbs_.end()) {
    return Status::NotFound(db  + " not found");
  }
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound(db + " not found");
  }
  if (sync_master_dbs_[p_info]->GetNumberOfSlaveNode() != 0 ||
      (sync_master_dbs_[p_info]->GetNumberOfSlaveNode() == 0 &&
       sync_slave_dbs_[p_info]->State() == kNoConnect)) {
    *role |= PIKA_ROLE_MASTER;
  }
  if (sync_slave_dbs_[p_info]->State() != ReplState::kNoConnect) {
    *role |= PIKA_ROLE_SLAVE;
  }
  // if role is not master or slave, the rest situations are all single
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

Status PikaReplicaManager::ActivateSyncSlaveDB(const RmNode& node, const ReplState& repl_state) {
  std::shared_lock l(dbs_rw_);
  const DBInfo& p_info = node.NodeDBInfo();
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound("Sync Slave DB " + node.ToString() + " not found");
  }
  ReplState ssp_state = sync_slave_dbs_[p_info]->State();
  if (ssp_state != ReplState::kNoConnect && ssp_state != ReplState::kDBNoConnect) {
    return Status::Corruption("Sync Slave DB in " + ReplStateMsg[ssp_state]);
  }
  std::string local_ip;
  Status s = SelectLocalIp(node.Ip(), node.Port(), &local_ip);
  if (s.ok()) {
    sync_slave_dbs_[p_info]->SetLocalIp(local_ip);
    sync_slave_dbs_[p_info]->Activate(node, repl_state);
  }
  return s;
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

Status PikaReplicaManager::SendRemoveSlaveNodeRequest(const std::string& db) {
  pstd::Status s;
  std::shared_lock l(dbs_rw_);
  DBInfo p_info(db);
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return Status::NotFound("Sync Slave DB " + p_info.ToString());
  } else {
    std::shared_ptr<SyncSlaveDB> s_db = sync_slave_dbs_[p_info];
    s = pika_repl_client_->SendRemoveSlaveNode(s_db->MasterIp(), s_db->MasterPort(), db, s_db->LocalIp());
    if (s.ok()) {
      s_db->SetReplState(ReplState::kDBNoConnect);
    }
  }

  if (s.ok()) {
    LOG(INFO) << "SlaveNode (" << db << ", stop sync success";
  } else {
    LOG(WARNING) << "SlaveNode (" << db << ", stop sync faild, " << s.ToString();
  }
  return s;
}

Status PikaReplicaManager::SendTrySyncRequest(const std::string& db_name) {
  BinlogOffset boffset;
  if (!g_pika_server->GetDBBinlogOffset(db_name, &boffset)) {
    LOG(WARNING) << "DB: " << db_name << ", Get DB binlog offset failed";
    return Status::Corruption("DB get binlog offset error");
  }

  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << ", NotFound";
    return Status::Corruption("Slave DB not found");
  }

  Status status =
      pika_repl_client_->SendTrySync(slave_db->MasterIp(), slave_db->MasterPort(), db_name,
                                     boffset, slave_db->LocalIp());

  if (status.ok()) {
    slave_db->SetReplState(ReplState::kWaitReply);
  } else {
    slave_db->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendDBTrySyncRequest failed " << status.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendDBSyncRequest(const std::string& db_name) {
  BinlogOffset boffset;
  if (!g_pika_server->GetDBBinlogOffset(db_name, &boffset)) {
    LOG(WARNING) << "DB: " << db_name << ", Get DB binlog offset failed";
    return Status::Corruption("DB get binlog offset error");
  }

  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name);
  if (!db) {
    LOG(WARNING) << "DB: " << db_name << " NotFound";
    return Status::Corruption("DB not found");
  }
  db->PrepareRsync();

  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << ", NotFound";
    return Status::Corruption("Slave DB not found");
  }

  Status status = pika_repl_client_->SendDBSync(slave_db->MasterIp(), slave_db->MasterPort(),
                                                    db_name, boffset, slave_db->LocalIp());

  Status s;
  if (status.ok()) {
    slave_db->SetReplState(ReplState::kWaitReply);
  } else {
    slave_db->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendDBSync failed " << status.ToString();
  }
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendBinlogSyncAckRequest(const std::string& db, const LogOffset& ack_start,
                                                    const LogOffset& ack_end, bool is_first_send) {
  std::shared_ptr<SyncSlaveDB> slave_db = GetSyncSlaveDBByName(DBInfo(db));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db << ":, NotFound";
    return Status::Corruption("Slave DB not found");
  }
  return pika_repl_client_->SendBinlogSync(slave_db->MasterIp(), slave_db->MasterPort(), db,
                                           ack_start, ack_end, slave_db->LocalIp(), is_first_send);
}

Status PikaReplicaManager::CloseReplClientConn(const std::string& ip, int32_t port) {
  return pika_repl_client_->Close(ip, port);
}

Status PikaReplicaManager::SendSlaveBinlogChipsRequest(const std::string& ip, int port,
                                                       const std::vector<WriteTask>& tasks) {
  return pika_repl_server_->SendSlaveBinlogChips(ip, port, tasks);
}

std::shared_ptr<SyncMasterDB> PikaReplicaManager::GetSyncMasterDBByName(const DBInfo& p_info) {
  std::shared_lock l(dbs_rw_);
  if (sync_master_dbs_.find(p_info) == sync_master_dbs_.end()) {
    return nullptr;
  }
  return sync_master_dbs_[p_info];
}

std::shared_ptr<SyncSlaveDB> PikaReplicaManager::GetSyncSlaveDBByName(const DBInfo& p_info) {
  std::shared_lock l(dbs_rw_);
  if (sync_slave_dbs_.find(p_info) == sync_slave_dbs_.end()) {
    return nullptr;
  }
  return sync_slave_dbs_[p_info];
}

Status PikaReplicaManager::RunSyncSlaveDBStateMachine() {
  std::shared_lock l(dbs_rw_);
  for (const auto& item : sync_slave_dbs_) {
    DBInfo p_info = item.first;
    std::shared_ptr<SyncSlaveDB> s_db = item.second;
    if (s_db->State() == ReplState::kTryConnect) {
      SendTrySyncRequest(p_info.db_name_);
    } else if (s_db->State() == ReplState::kTryDBSync) {
      SendDBSyncRequest(p_info.db_name_);
    } else if (s_db->State() == ReplState::kWaitReply) {
      continue;
    } else if (s_db->State() == ReplState::kWaitDBSync) {
      Status s = s_db->ActivateRsync();
      if (!s.ok()) {
        LOG(WARNING) << "Slave DB: " << s_db->DBName() << " rsync failed! full synchronization will be retried later";
        continue;
      }

      std::shared_ptr<DB> db =
          g_pika_server->GetDB(p_info.db_name_);
      if (db) {
        if (s_db->IsRsyncExited()) {
          db->TryUpdateMasterOffset();
        }
      } else {
        LOG(WARNING) << "DB not found, DB Name: " << p_info.db_name_;
      }
    } else if (s_db->State() == ReplState::kConnected || s_db->State() == ReplState::kNoConnect ||
               s_db->State() == ReplState::kDBNoConnect) {
      continue;
    }
  }
  return Status::OK();
}

void PikaReplicaManager::FindCommonMaster(std::string* master) {
  std::shared_lock l(dbs_rw_);
  std::string common_master_ip;
  int common_master_port = 0;
  for (auto& iter : sync_slave_dbs_) {
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
  std::shared_lock l(dbs_rw_);
  std::stringstream tmp_stream;
  tmp_stream << "Master DB(" << sync_master_dbs_.size() << "):"
             << "\r\n";
  for (auto& iter : sync_master_dbs_) {
    tmp_stream << " DB " << iter.second->SyncDBInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";
  }
  tmp_stream << "Slave DB(" << sync_slave_dbs_.size() << "):"
             << "\r\n";
  for (auto& iter : sync_slave_dbs_) {
    tmp_stream << " DB " << iter.second->SyncDBInfo().ToString() << "\r\n"
               << iter.second->ToStringStatus() << "\r\n";
  }
  info->append(tmp_stream.str());
}
