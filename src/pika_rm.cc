// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rm.h"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <glog/logging.h>

#include "pink/include/pink_cli.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"

extern PikaConf *g_pika_conf;
extern PikaReplicaManager* g_pika_rm;
extern PikaServer *g_pika_server;

/* SyncPartition */

SyncPartition::SyncPartition(const std::string& table_name, uint32_t partition_id)
  : partition_info_(table_name, partition_id) {
}

std::string SyncPartition::PartitionName() {
  if (g_pika_conf->classic_mode()) {
    return partition_info_.table_name_;
  } else {
    return partition_info_.ToString();
  }
}

/* SyncMasterPartition*/

SyncMasterPartition::SyncMasterPartition(const std::string& table_name, uint32_t partition_id)
    : SyncPartition(table_name, partition_id),
      session_id_(0),
      coordinator_(table_name, partition_id) {
}

int SyncMasterPartition::GetNumberOfSlaveNode() {
  return coordinator_.SyncPros().SlaveSize();
}

bool SyncMasterPartition::CheckSlaveNodeExist(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return false;
  }
  return true;
}

Status SyncMasterPartition::GetSlaveNodeSession(
    const std::string& ip, int port, int32_t* session) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("slave " + ip + ":" + std::to_string(port) + " not found");
  }

  slave_ptr->Lock();
  *session = slave_ptr->SessionId();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = coordinator_.AddSlaveNode(ip, port, session_id);
  if (!s.ok()) {
    LOG(WARNING) << "Add Slave Node Failed, partition: " << SyncPartitionInfo().ToString() << ", ip_port: "<< ip << ":" << port;
    return s;
  }
  LOG(INFO) << "Add Slave Node, partition: " << SyncPartitionInfo().ToString() << ", ip_port: "<< ip << ":" << port;
  return Status::OK();
}

Status SyncMasterPartition::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = coordinator_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    LOG(WARNING) << "Remove Slave Node Failed, Partition: " << SyncPartitionInfo().ToString()
      << ", ip_port: "<< ip << ":" << port;
    return s;
  }
  LOG(INFO) << "Remove Slave Node, Partition: " << SyncPartitionInfo().ToString()
        << ", ip_port: "<< ip << ":" << port;
  return Status::OK();
}

Status SyncMasterPartition::ActivateSlaveBinlogSync(const std::string& ip,
                                                    int port,
                                                    const BinlogOffset& offset) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  {
    slash::MutexLock l(&slave_ptr->slave_mu);
    slave_ptr->slave_state = kSlaveBinlogSync;
    slave_ptr->sent_offset = offset;
    slave_ptr->acked_offset = offset;
    // read binlog file from file
    Status s = slave_ptr->InitBinlogFileReader(Logger(), offset);
    if (!s.ok()) {
      return Status::Corruption("Init binlog file reader failed" + s.ToString());
    }
    slave_ptr->b_state = kReadFromFile;
  }

  Status s = SyncBinlogToWq(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status SyncMasterPartition::SyncBinlogToWq(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  ReadBinlogFileToWq(slave_ptr);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::ActivateSlaveDbSync(const std::string& ip, int port) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->slave_state = kSlaveDbSync;
  // invoke db sync
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
  int cnt = slave_ptr->sync_win.Remainings();
  std::shared_ptr<PikaBinlogReader> reader = slave_ptr->binlog_reader;
  std::vector<WriteTask> tasks;
  for (int i = 0; i < cnt; ++i) {
    std::string msg;
    uint32_t filenum;
    uint64_t offset;
    Status s = reader->Get(&msg, &filenum, &offset);
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      LOG(WARNING) << SyncPartitionInfo().ToString()
        << " Read Binlog error : " << s.ToString();
      return s;
    }
    slave_ptr->sync_win.Push(SyncWinItem(filenum, offset));

    BinlogOffset sent_offset = BinlogOffset(filenum, offset);
    slave_ptr->sent_offset = sent_offset;
    slave_ptr->SetLastSendTime(slash::NowMicros());
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->TableName(), slave_ptr->PartitionId(), slave_ptr->SessionId());
    WriteTask task(rm_node, BinlogChip(sent_offset, msg));
    tasks.push_back(task);
  }

  if (!tasks.empty()) {
    g_pika_rm->ProduceWriteQueue(slave_ptr->Ip(), slave_ptr->Port(), tasks);
  }
  return Status::OK();
}

Status SyncMasterPartition::ConsistencyUpdateSlave(const std::string& ip, int port, const BinlogOffset& start, const BinlogOffset& end) {
  Status s = coordinator_.UpdateSlave(ip, port, start, end);
  if (!s.ok()) {
    LOG(WARNING) << SyncPartitionInfo().ToString() << s.ToString();
    return s;
  }
  return Status::OK();
}

Status SyncMasterPartition::GetSlaveSyncBinlogInfo(const std::string& ip,
                                                   int port,
                                                   BinlogOffset* sent_offset,
                                                   BinlogOffset* acked_offset) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *sent_offset = slave_ptr->sent_offset;
  *acked_offset = slave_ptr->acked_offset;
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::GetSlaveState(const std::string& ip,
                                          int port,
                                          SlaveState* const slave_state) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *slave_state = slave_ptr->slave_state;
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::WakeUpSlaveBinlogSync() {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    slash::MutexLock l(&slave_ptr->slave_mu);
    if (slave_ptr->sent_offset == slave_ptr->acked_offset) {
      ReadBinlogFileToWq(slave_ptr);
    }
  }
  return Status::OK();
}

Status SyncMasterPartition::SetLastSendTime(const std::string& ip, int port, uint64_t time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->SetLastSendTime(time);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::GetLastSendTime(const std::string& ip, int port, uint64_t* time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *time = slave_ptr->LastSendTime();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::SetLastRecvTime(const std::string& ip, int port, uint64_t time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  slave_ptr->SetLastRecvTime(time);
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::GetLastRecvTime(const std::string& ip, int port, uint64_t* time) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  slave_ptr->Lock();
  *time = slave_ptr->LastRecvTime();
  slave_ptr->Unlock();

  return Status::OK();
}

Status SyncMasterPartition::GetSafetyPurgeBinlog(std::string* safety_purge) {
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
      slash::MutexLock l(&slave_ptr->slave_mu);
      if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync
        && slave_ptr->acked_offset.filenum > 0) {
        purge_max = std::min(slave_ptr->acked_offset.filenum - 1, purge_max);
      } else {
        success = false;
        break;
      }
    }
  }
  *safety_purge = (success ? kBinlogPrefix + std::to_string(static_cast<int32_t>(purge_max)) : "none");
  return Status::OK();
}

bool SyncMasterPartition::BinlogCloudPurge(uint32_t index) {
  BinlogOffset boffset;
  Status s = Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    return false;
  }
  if (index > boffset.filenum - 10) {  // remain some more
    return false;
  } else {
    std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
    for (const auto& slave_iter : slaves) {
      std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
      slash::MutexLock l(&slave_ptr->slave_mu);
      if (slave_ptr->slave_state == SlaveState::kSlaveDbSync) {
        return false;
      } else if (slave_ptr->slave_state == SlaveState::kSlaveBinlogSync) {
        if (index >= slave_ptr->acked_offset.filenum) {
          return false;
        }
      }
    }
  }
  return true;
}

Status SyncMasterPartition::CheckSyncTimeout(uint64_t now) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  std::vector<Node> to_del;
  for (auto& slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    slash::MutexLock l(&slave_ptr->slave_mu);
    if (slave_ptr->LastRecvTime() + kRecvKeepAliveTimeout < now) {
      to_del.push_back(Node(slave_ptr->Ip(), slave_ptr->Port()));
    } else if (slave_ptr->LastSendTime() + kSendKeepAliveTimeout < now) {
      std::vector<WriteTask> task;
      RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->TableName(), slave_ptr->PartitionId(), slave_ptr->SessionId());
      WriteTask empty_task(rm_node, BinlogChip(BinlogOffset(0, 0), ""));
      task.push_back(empty_task);
      Status s = g_pika_rm->SendSlaveBinlogChipsRequest(slave_ptr->Ip(), slave_ptr->Port(), task);
      slave_ptr->SetLastSendTime(now);
      if (!s.ok()) {
        LOG(INFO)<< "Send ping failed: " << s.ToString();
        return Status::Corruption("Send ping failed: " + slave_ptr->Ip() + ":" + std::to_string(slave_ptr->Port()));
      }
    }
  }

  for (auto& node : to_del) {
    coordinator_.SyncPros().RemoveSlaveNode(node.Ip(), node.Port());
    LOG(WARNING) << SyncPartitionInfo().ToString() << " Master del Recv Timeout slave success " << node.ToString();
  }
  return Status::OK();
}

std::string SyncMasterPartition::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << " Current Master Session: " << session_id_ << "\r\n";
  tmp_stream << " ConsistencyLogs size: " <<
    (coordinator_.MemLogger() == nullptr ? 0 : coordinator_.MemLogger()->Size()) << "\r\n";

  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  int i = 0;
  for (auto slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    slash::MutexLock l(&slave_ptr->slave_mu);
    tmp_stream << "  slave[" << i << "]: "  << slave_ptr->ToString() <<
      "\r\n" << slave_ptr->ToStringStatus();
    i++;
  }
  return tmp_stream.str();
}

void SyncMasterPartition::GetValidSlaveNames(std::vector<std::string>* slavenames) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();
  for (auto slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    slash::MutexLock l(&slave_ptr->slave_mu);
    if (slave_ptr->slave_state != kSlaveBinlogSync) {
      continue;
    }
    std::string name = slave_ptr->Ip() + ":" + std::to_string(slave_ptr->Port());
    slavenames->push_back(name);
  }
}

Status SyncMasterPartition::GetInfo(std::string* info) {
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves = GetAllSlaveNodes();

  std::stringstream tmp_stream;
  tmp_stream << "  Role: Master" << "\r\n";
  tmp_stream << "  connected_slaves: " << slaves.size() << "\r\n";
  int i = 0;
  for (auto slave_iter : slaves) {
    std::shared_ptr<SlaveNode> slave_ptr = slave_iter.second;
    slash::MutexLock l(&slave_ptr->slave_mu);
    tmp_stream << "  slave[" << i++ << "]: "
      << slave_ptr->Ip()  << ":" << std::to_string(slave_ptr->Port()) << "\r\n";
    tmp_stream << "  replication_status: " << SlaveStateMsg[slave_ptr->slave_state] << "\r\n";
    if (slave_ptr->slave_state == kSlaveBinlogSync) {
      BinlogOffset binlog_offset;
      Status s = Logger()->GetProducerStatus(&(binlog_offset.filenum), &(binlog_offset.offset));
      if (!s.ok()) {
        return s;
      }
      uint64_t lag = (binlog_offset.filenum - slave_ptr->acked_offset.filenum) *
        g_pika_conf->binlog_file_size()
        + (binlog_offset.offset - slave_ptr->acked_offset.offset);
      tmp_stream << "  lag: " << lag << "\r\n";
    }
  }
  info->append(tmp_stream.str());
  return Status::OK();
}

int32_t SyncMasterPartition::GenSessionId() {
  slash::MutexLock ml(&session_mu_);
  return session_id_++;
}

bool SyncMasterPartition::CheckSessionId(const std::string& ip, int port,
                                         const std::string& table_name,
                                         uint64_t partition_id, int session_id) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    LOG(WARNING)<< "Check SessionId Get Slave Node Error: "
        << ip << ":" << port << "," << table_name << "_" << partition_id;
    return false;
  }

  slash::MutexLock l(&slave_ptr->slave_mu);
  if (session_id != slave_ptr->SessionId()) {
    LOG(WARNING)<< "Check SessionId Mismatch: " << ip << ":" << port << ", "
        << table_name << "_" << partition_id << " expected_session: " << session_id
        << ", actual_session:" << slave_ptr->SessionId();
    return false;
  }
  return true;
}

Status SyncMasterPartition::ConsistencyProposeLog(
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  return coordinator_.ProposeLog(cmd_ptr, conn_ptr, resp_ptr);
}

Status SyncMasterPartition::ConsistencySanityCheck() {
  return coordinator_.CheckEnoughFollower();
}

std::shared_ptr<SlaveNode> SyncMasterPartition::GetSlaveNode(const std::string& ip, int port) {
  return coordinator_.SyncPros().GetSlaveNode(ip, port);
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>>
SyncMasterPartition::GetAllSlaveNodes() {
  return coordinator_.SyncPros().GetAllSlaveNodes();
}

/* SyncSlavePartition */
SyncSlavePartition::SyncSlavePartition(const std::string& table_name,
                                       uint32_t partition_id)
  : SyncPartition(table_name, partition_id),
    m_info_(),
    repl_state_(kNoConnect),
    local_ip_("") {
  m_info_.SetLastRecvTime(slash::NowMicros());
}

void SyncSlavePartition::SetReplState(const ReplState& repl_state) {
  if (repl_state == ReplState::kNoConnect) {
    // deactivate
    Deactivate();
    return;
  }
  slash::MutexLock l(&partition_mu_);
  repl_state_ = repl_state;
}

ReplState SyncSlavePartition::State() {
  slash::MutexLock l(&partition_mu_);
  return repl_state_;
}

void SyncSlavePartition::SetLastRecvTime(uint64_t time) {
  slash::MutexLock l(&partition_mu_);
  m_info_.SetLastRecvTime(time);
}

uint64_t SyncSlavePartition::LastRecvTime() {
  slash::MutexLock l(&partition_mu_);
  return m_info_.LastRecvTime();
}

Status SyncSlavePartition::CheckSyncTimeout(uint64_t now) {
  slash::MutexLock l(&partition_mu_);
  // no need to do session keepalive return ok
  if (repl_state_ != ReplState::kWaitDBSync && repl_state_ != ReplState::kConnected) {
    return Status::OK();
  }
  if (m_info_.LastRecvTime() + kRecvKeepAliveTimeout < now) {
    m_info_ = RmNode();
    repl_state_ = ReplState::kTryConnect;
    g_pika_server->SetLoopPartitionStateMachine(true);
  }
  return Status::OK();
}

Status SyncSlavePartition::GetInfo(std::string* info) {
  std::string tmp_str = "  Role: Slave\r\n";
  tmp_str += "  master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n";
  info->append(tmp_str);
  return Status::OK();
}

void SyncSlavePartition::Activate(const RmNode& master, const ReplState& repl_state) {
  slash::MutexLock l(&partition_mu_);
  m_info_ = master;
  repl_state_ = repl_state;
  m_info_.SetLastRecvTime(slash::NowMicros());
}

void SyncSlavePartition::Deactivate() {
  slash::MutexLock l(&partition_mu_);
  m_info_ = RmNode();
  repl_state_ = ReplState::kNoConnect;
}

std::string SyncSlavePartition::ToStringStatus() {
  return "  Master: " + MasterIp() + ":" + std::to_string(MasterPort()) + "\r\n" +
    "  SessionId: " + std::to_string(MasterSessionId()) + "\r\n" +
    "  SyncStatus " + ReplStateMsg[repl_state_] + "\r\n";
}

const std::string& SyncSlavePartition::MasterIp() {
  slash::MutexLock l(&partition_mu_);
  return m_info_.Ip();
}

int SyncSlavePartition::MasterPort() {
  slash::MutexLock l(&partition_mu_);
  return m_info_.Port();
}

void SyncSlavePartition::SetMasterSessionId(int32_t session_id) {
  slash::MutexLock l(&partition_mu_);
  m_info_.SetSessionId(session_id);
}

int32_t SyncSlavePartition::MasterSessionId() {
  slash::MutexLock l(&partition_mu_);
  return m_info_.SessionId();
}

void SyncSlavePartition::SetLocalIp(const std::string& local_ip) {
  slash::MutexLock l(&partition_mu_);
  local_ip_ = local_ip;
}

std::string SyncSlavePartition::LocalIp() {
  slash::MutexLock l(&partition_mu_);
  return local_ip_;
}

/* SyncWindow */

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
}

bool SyncWindow::Update(const SyncWinItem& start_item,
    const SyncWinItem& end_item, BinlogOffset* acked_offset) {
  size_t start_pos = win_.size(), end_pos = win_.size();
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (start_pos == win_.size() || end_pos == win_.size()) {
    LOG(WARNING) << "Ack offset Start: " <<
      start_item.ToString() << "End: " << end_item.ToString() <<
      " not found in binlog controller window." <<
      std::endl << "window status "<< std::endl << ToStringStatus();
    return false;
  }
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
  }
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.pop_front();
    } else {
      break;
    }
  }
  return true;
}

int SyncWindow::Remainings() {
  std::size_t remaining_size = g_pika_conf->sync_window_size() - win_.size();
  return remaining_size > 0? remaining_size:0 ;
}

/* PikaReplicaManger */

PikaReplicaManager::PikaReplicaManager()
    : last_meta_sync_timestamp_(0) {
  std::set<std::string> ips;
  ips.insert("0.0.0.0");
  int port = g_pika_conf->port() + kPortShiftReplServer;
  pika_repl_client_ = new PikaReplClient(3000, 60);
  pika_repl_server_ = new PikaReplServer(ips, port, 3000);
  InitPartition();
  pthread_rwlock_init(&partitions_rw_, NULL);
}

PikaReplicaManager::~PikaReplicaManager() {
  delete pika_repl_client_;
  delete pika_repl_server_;
  pthread_rwlock_destroy(&partitions_rw_);
}

void PikaReplicaManager::Start() {
  int ret = 0;
  ret = pika_repl_client_->Start();
  if (ret != pink::kSuccess) {
    LOG(FATAL) << "Start Repl Client Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }

  ret = pika_repl_server_->Start();
  if (ret != pink::kSuccess) {
    LOG(FATAL) << "Start Repl Server Error: " << ret << (ret == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
}

void PikaReplicaManager::Stop() {
  pika_repl_client_->Stop();
  pika_repl_server_->Stop();
}

void PikaReplicaManager::InitPartition() {
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  for (const auto& table : table_structs) {
    const std::string& table_name = table.table_name;
    for (const auto& partition_id : table.partition_ids) {
      sync_master_partitions_[PartitionInfo(table_name, partition_id)]
        = std::make_shared<SyncMasterPartition>(table_name, partition_id);
      sync_slave_partitions_[PartitionInfo(table_name, partition_id)]
        = std::make_shared<SyncSlavePartition>(table_name, partition_id);
    }
  }
}

void PikaReplicaManager::ProduceWriteQueue(const std::string& ip, int port, const std::vector<WriteTask>& tasks) {
  slash::MutexLock l(&write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  for (auto& task : tasks) {
    write_queues_[index].push(task);
  }
}

int PikaReplicaManager::ConsumeWriteQueue() {
  std::vector<std::string> to_delete;
  std::unordered_map<std::string, std::vector<std::vector<WriteTask>>> to_send_map;
  int counter = 0;
  {
    slash::MutexLock l(&write_queue_mu_);
    std::vector<std::string> to_delete;
    for (auto& iter : write_queues_) {
      std::queue<WriteTask>& queue = iter.second;
      for (int i = 0; i < kBinlogSendPacketNum; ++i) {
        if (queue.empty()) {
          break;
        }
        size_t batch_index = queue.size() > kBinlogSendBatchNum ? kBinlogSendBatchNum : queue.size();
        std::vector<WriteTask> to_send;
        for (size_t i = 0; i < batch_index; ++i) {
          to_send.push_back(queue.front());
          queue.pop();
          counter++;
        }
        to_send_map[iter.first].push_back(std::move(to_send));
      }
    }
  }

  for (auto& iter : to_send_map) {
    std::string ip;
    int port = 0;
    if (!slash::ParseIpPortString(iter.first, ip, port)) {
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
      slash::MutexLock l(&write_queue_mu_);
      for (auto& del_queue : to_delete) {
        write_queues_.erase(del_queue);
      }
    }
  }
  return counter;
}

void PikaReplicaManager::DropItemInWriteQueue(const std::string& ip, int port) {
  slash::MutexLock l(&write_queue_mu_);
  std::string index = ip + ":" + std::to_string(port);
  write_queues_.erase(index);
}

void PikaReplicaManager::ScheduleReplServerBGTask(pink::TaskFunc func, void* arg) {
  pika_repl_server_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleReplClientBGTask(pink::TaskFunc func, void* arg) {
  pika_repl_client_->Schedule(func, arg);
}

void PikaReplicaManager::ScheduleWriteBinlogTask(const std::string& table_partition,
        const std::shared_ptr<InnerMessage::InnerResponse> res,
        std::shared_ptr<pink::PbConn> conn,
        void* res_private_data) {
  pika_repl_client_->ScheduleWriteBinlogTask(table_partition, res, conn, res_private_data);
}

void PikaReplicaManager::ScheduleWriteDBTask(const std::string& dispatch_key,
        PikaCmdArgsType* argv, BinlogItem* binlog_item,
        const std::string& table_name, uint32_t partition_id) {
  pika_repl_client_->ScheduleWriteDBTask(dispatch_key, argv, binlog_item, table_name, partition_id);
}

void PikaReplicaManager::ReplServerRemoveClientConn(int fd) {
  pika_repl_server_->RemoveClientConn(fd);
}

void PikaReplicaManager::ReplServerUpdateClientConnMap(const std::string& ip_port,
                                                       int fd) {
  pika_repl_server_->UpdateClientConnMap(ip_port, fd);
}

Status PikaReplicaManager::UpdateSyncBinlogStatus(const RmNode& slave, const BinlogOffset& range_start, const BinlogOffset& range_end) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s = partition->ConsistencyUpdateSlave(slave.Ip(), slave.Port(), range_start, range_end);
  if (!s.ok()) {
    return s;
  }
  s = partition->SyncBinlogToWq(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::LostConnection(const std::string& ip, int port) {
  slash::RWLock l(&partitions_rw_, false);
  for (auto& iter : sync_master_partitions_) {
    std::shared_ptr<SyncMasterPartition> partition = iter.second;
    Status s = partition->RemoveSlaveNode(ip, port);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(WARNING) << "Lost Connection failed " << s.ToString();
    }
  }

  for (auto& iter : sync_slave_partitions_) {
    std::shared_ptr<SyncSlavePartition> partition = iter.second;
    if (partition->MasterIp() == ip && partition->MasterPort() == port) {
      partition->Deactivate();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::WakeUpBinlogSync() {
  slash::RWLock l(&partitions_rw_, false);
  for (auto& iter : sync_master_partitions_) {
    std::shared_ptr<SyncMasterPartition> partition = iter.second;
    Status s = partition->WakeUpSlaveBinlogSync();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckSyncTimeout(uint64_t now) {
  slash::RWLock l(&partitions_rw_, false);

  for (auto& iter : sync_master_partitions_) {
    std::shared_ptr<SyncMasterPartition> partition = iter.second;
    Status s = partition->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  for (auto& iter : sync_slave_partitions_) {
    std::shared_ptr<SyncSlavePartition> partition = iter.second;
    Status s = partition->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::CheckPartitionRole(
    const std::string& table, uint32_t partition_id, int* role) {
  slash::RWLock l(&partitions_rw_, false);
  *role = 0;
  PartitionInfo p_info(table, partition_id);
  if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()) {
    return Status::NotFound(table + std::to_string(partition_id) + " not found");
  }
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound(table + std::to_string(partition_id) + " not found");
  }
  if (sync_master_partitions_[p_info]->GetNumberOfSlaveNode() != 0) {
    *role |= PIKA_ROLE_MASTER;
  }
  if (sync_slave_partitions_[p_info]->State() == ReplState::kConnected) {
    *role |= PIKA_ROLE_SLAVE;
  }
  // if role is not master or slave, the rest situations are all single
  return Status::OK();
}

Status PikaReplicaManager::GetPartitionInfo(
    const std::string& table, uint32_t partition_id, std::string* info) {
  int role = 0;
  std::string tmp_res;
  Status s = CheckPartitionRole(table, partition_id, &role);
  if (!s.ok()) {
    return s;
  }

  bool add_divider_line = ((role & PIKA_ROLE_MASTER) && (role & PIKA_ROLE_SLAVE));
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table, partition_id);
  if (role & PIKA_ROLE_MASTER) {
    if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()) {
     return Status::NotFound(table + std::to_string(partition_id) + " not found");
    }
    Status s = sync_master_partitions_[p_info]->GetInfo(info);
    if (!s.ok()) {
      return s;
    }
  }
  if (add_divider_line) {
    info->append("  -----------\r\n");
  }
  if (role & PIKA_ROLE_SLAVE) {
    if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
      return Status::NotFound(table + std::to_string(partition_id) + " not found");
    }
    Status s = sync_slave_partitions_[p_info]->GetInfo(info);
    if (!s.ok()) {
      return s;
    }
  }
  info->append("\r\n");
  return Status::OK();
}

Status PikaReplicaManager::SelectLocalIp(const std::string& remote_ip,
                                         const int remote_port,
                                         std::string* const local_ip) {
  pink::PinkCli* cli = pink::NewRedisCli();
  cli->set_connect_timeout(1500);
  if ((cli->Connect(remote_ip, remote_port, "")).ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), (struct sockaddr*) &laddr, &llen);
    std::string tmp_ip(inet_ntoa(laddr.sin_addr));
    *local_ip = tmp_ip;
    cli->Close();
    delete cli;
  } else {
    LOG(WARNING) << "Failed to connect remote node("
      << remote_ip << ":" << remote_port << ")";
    delete cli;
    return Status::Corruption("connect remote node error");
  }
  return Status::OK();
}

Status PikaReplicaManager::ActivateSyncSlavePartition(const RmNode& node,
                                                      const ReplState& repl_state) {
  slash::RWLock l(&partitions_rw_, false);
  const PartitionInfo& p_info = node.NodePartitionInfo();
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + node.ToString() + " not found");
  }
  ReplState ssp_state  = sync_slave_partitions_[p_info]->State();
  if (ssp_state != ReplState::kNoConnect) {
    return Status::Corruption("Sync Slave partition in " + ReplStateMsg[ssp_state]);
  }
  std::string local_ip;
  Status s = SelectLocalIp(node.Ip(), node.Port(), &local_ip);
  if (s.ok()) {
    sync_slave_partitions_[p_info]->SetLocalIp(local_ip);
    sync_slave_partitions_[p_info]->Activate(node, repl_state);
  }
  return s;
}

Status PikaReplicaManager::DeactivateSyncSlavePartition(const PartitionInfo& p_info) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  }
  sync_slave_partitions_[p_info]->Deactivate();
  return Status::OK();
}

Status PikaReplicaManager::SendMetaSyncRequest() {
  Status s;
  int now = time(NULL);
  if (now - last_meta_sync_timestamp_ >= PIKA_META_SYNC_MAX_WAIT_TIME) {
    s = pika_repl_client_->SendMetaSync();
    if (s.ok()) {
      last_meta_sync_timestamp_ = now;
    }
  }
  return s;
}

Status PikaReplicaManager::SendRemoveSlaveNodeRequest(const std::string& table,
                                                      uint32_t partition_id) {
  slash::Status s;
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table, partition_id);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  } else {
    std::shared_ptr<SyncSlavePartition> s_partition = sync_slave_partitions_[p_info];
    s = pika_repl_client_->SendRemoveSlaveNode(s_partition->MasterIp(),
        s_partition->MasterPort(), table, partition_id, s_partition->LocalIp());
    if (s.ok()) {
      s_partition->Deactivate();
    }
  }

  if (s.ok()) {
    LOG(INFO) << "SlaveNode (" << table << ":" << partition_id
      << "), stop sync success";
  } else {
    LOG(WARNING) << "SlaveNode (" << table << ":" << partition_id
      << "), stop sync faild, " << s.ToString();
  }
  return s;
}

Status PikaReplicaManager::SendPartitionTrySyncRequest(
        const std::string& table_name, size_t partition_id) {
  BinlogOffset boffset;
  if (!g_pika_server->GetTablePartitionBinlogOffset(
              table_name, partition_id, &boffset)) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id
        << ",  Get partition binlog offset failed";
    return Status::Corruption("Partition get binlog offset error");
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
      GetSyncSlavePartitionByName(PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table_name << ":" << partition_id
        << ", NotFound";
    return Status::Corruption("Slave Partition not found");
  }

  Status status = pika_repl_client_->SendPartitionTrySync(slave_partition->MasterIp(),
                                                          slave_partition->MasterPort(),
                                                          table_name, partition_id, boffset,
                                                          slave_partition->LocalIp());

  Status s;
  if (status.ok()) {
    slave_partition->SetReplState(ReplState::kWaitReply);
  } else {
    slave_partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendPartitionTrySyncRequest failed " << status.ToString();
  }
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendPartitionDBSyncRequest(
        const std::string& table_name, size_t partition_id) {
  BinlogOffset boffset;
  if (!g_pika_server->GetTablePartitionBinlogOffset(
              table_name, partition_id, &boffset)) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id
        << ",  Get partition binlog offset failed";
    return Status::Corruption("Partition get binlog offset error");
  }

  std::shared_ptr<Partition> partition =
    g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id
        << ", NotFound";
    return Status::Corruption("Partition not found");
  }
  partition->PrepareRsync();

  std::shared_ptr<SyncSlavePartition> slave_partition =
      GetSyncSlavePartitionByName(PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table_name << ":" << partition_id
        << ", NotFound";
    return Status::Corruption("Slave Partition not found");
  }

  Status status = pika_repl_client_->SendPartitionDBSync(slave_partition->MasterIp(),
                                                         slave_partition->MasterPort(),
                                                         table_name, partition_id, boffset,
                                                         slave_partition->LocalIp());

  Status s;
  if (status.ok()) {
    slave_partition->SetReplState(ReplState::kWaitReply);
  } else {
    slave_partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "SendPartitionDbSync failed " << status.ToString();
  }
  if (!s.ok()) {
    LOG(WARNING) << s.ToString();
  }
  return status;
}

Status PikaReplicaManager::SendPartitionBinlogSyncAckRequest(
        const std::string& table, uint32_t partition_id,
        const BinlogOffset& ack_start, const BinlogOffset& ack_end,
        bool is_first_send) {
  std::shared_ptr<SyncSlavePartition> slave_partition =
      GetSyncSlavePartitionByName(PartitionInfo(table, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table << ":" << partition_id
        << ", NotFound";
    return Status::Corruption("Slave Partition not found");
  }
  return pika_repl_client_->SendPartitionBinlogSync(
          slave_partition->MasterIp(), slave_partition->MasterPort(),
          table, partition_id, ack_start, ack_end, slave_partition->LocalIp(),
          is_first_send);
}

Status PikaReplicaManager::CloseReplClientConn(const std::string& ip, int32_t port) {
  return pika_repl_client_->Close(ip, port);
}

Status PikaReplicaManager::SendSlaveBinlogChipsRequest(const std::string& ip,
                                                       int port,
                                                       const std::vector<WriteTask>& tasks) {
  return pika_repl_server_->SendSlaveBinlogChips(ip, port, tasks);
}

std::shared_ptr<SyncMasterPartition>
PikaReplicaManager::GetSyncMasterPartitionByName(const PartitionInfo& p_info) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()) {
    return nullptr;
  }
  return sync_master_partitions_[p_info];
}

std::shared_ptr<SyncSlavePartition>
PikaReplicaManager::GetSyncSlavePartitionByName(const PartitionInfo& p_info) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return nullptr;
  }
  return sync_slave_partitions_[p_info];
}

Status PikaReplicaManager::RunSyncSlavePartitionStateMachine() {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& item : sync_slave_partitions_) {
    PartitionInfo p_info = item.first;
    std::shared_ptr<SyncSlavePartition> s_partition = item.second;
    if (s_partition->State() == ReplState::kTryConnect) {
      SendPartitionTrySyncRequest(p_info.table_name_, p_info.partition_id_);
    } else if (s_partition->State() == ReplState::kTryDBSync) {
      SendPartitionDBSyncRequest(p_info.table_name_, p_info.partition_id_);
    } else if (s_partition->State() == ReplState::kWaitReply) {
      continue;
    } else if (s_partition->State() == ReplState::kWaitDBSync) {
      std::shared_ptr<Partition> partition =
          g_pika_server->GetTablePartitionById(
                  p_info.table_name_, p_info.partition_id_);
      if (partition) {
        partition->TryUpdateMasterOffset();
      } else {
        LOG(WARNING) << "Partition not found, Table Name: "
          << p_info.table_name_ << " Partition Id: " << p_info.partition_id_;
      }
    } else if (s_partition->State() == ReplState::kConnected
      || s_partition->State() == ReplState::kNoConnect) {
      continue;
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSyncPartitionSanityCheck(const std::set<PartitionInfo>& p_infos) {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& p_info : p_infos) {
    if (sync_master_partitions_.find(p_info) != sync_master_partitions_.end()
      || sync_slave_partitions_.find(p_info) != sync_slave_partitions_.end()) {
      LOG(WARNING) << "sync partition: " << p_info.ToString() << " exist";
      return Status::Corruption("sync partition " + p_info.ToString()
          + " exist");
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSyncPartition(
        const std::set<PartitionInfo>& p_infos) {
  Status s = AddSyncPartitionSanityCheck(p_infos);
  if (!s.ok()) {
    return s;
  }

  slash::RWLock l(&partitions_rw_, true);
  for (const auto& p_info : p_infos) {
    sync_master_partitions_[p_info] =
      std::make_shared<SyncMasterPartition>(p_info.table_name_,
          p_info.partition_id_);
    sync_slave_partitions_[p_info] =
      std::make_shared<SyncSlavePartition>(p_info.table_name_,
          p_info.partition_id_);
  }
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncPartitionSanityCheck(
    const std::set<PartitionInfo>& p_infos) {
  slash::RWLock l(&partitions_rw_, false);
  for (const auto& p_info : p_infos) {
    if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()
      || sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
      LOG(WARNING) << "sync partition: " << p_info.ToString() << " not found";
      return Status::Corruption("sync partition " + p_info.ToString()
              + " not found");
    }

    if (sync_master_partitions_[p_info]->GetNumberOfSlaveNode() != 0) {
      LOG(WARNING) << "sync master partition: " << p_info.ToString()
          << " in syncing";
      return Status::Corruption("sync master partition " + p_info.ToString()
              + " in syncing");
    }

    ReplState state = sync_slave_partitions_[p_info]->State();
    if (state != kNoConnect && state != kError) {
      LOG(WARNING) << "sync slave partition: " << p_info.ToString()
          << " in " << ReplStateMsg[state] + " state";
      return Status::Corruption("sync slave partition " + p_info.ToString()
              + " in " + ReplStateMsg[state] + " state");
    }
  }
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncPartition(
        const std::set<PartitionInfo>& p_infos) {
  Status s = RemoveSyncPartitionSanityCheck(p_infos);
  if (!s.ok()) {
    return s;
  }

  slash::RWLock l(&partitions_rw_, true);
  for (const auto& p_info : p_infos) {
    if (sync_master_partitions_.find(p_info) != sync_master_partitions_.end()) {
      sync_master_partitions_[p_info]->StableLogger()->Leave();
    }
    sync_master_partitions_.erase(p_info);
    sync_slave_partitions_.erase(p_info);
  }
  return Status::OK();
}

void PikaReplicaManager::FindCompleteReplica(std::vector<std::string>* replica) {
  std::unordered_map<std::string, size_t> replica_slotnum;
  slash::RWLock l(&partitions_rw_, false);
  for (auto& iter : sync_master_partitions_) {
    std::vector<std::string> names;
    iter.second->GetValidSlaveNames(&names);
    for (auto& name : names) {
      if (replica_slotnum.find(name) == replica_slotnum.end()) {
        replica_slotnum[name] = 0;
      }
      replica_slotnum[name]++;
    }
  }
  for (auto item : replica_slotnum) {
    if (item.second == sync_master_partitions_.size()) {
      replica->push_back(item.first);
    }
  }
}

void PikaReplicaManager::FindCommonMaster(std::string* master) {
  slash::RWLock l(&partitions_rw_, false);
  std::string common_master_ip;
  int common_master_port = 0;
  for (auto& iter : sync_slave_partitions_) {
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
  slash::RWLock l(&partitions_rw_, false);
  std::stringstream tmp_stream;
  tmp_stream << "Master partition(" << sync_master_partitions_.size() << "):" << "\r\n";
  for (auto& iter : sync_master_partitions_) {
    tmp_stream << " Partition " << iter.second->SyncPartitionInfo().ToString()
      << "\r\n" << iter.second->ToStringStatus() << "\r\n";
  }
  tmp_stream << "Slave partition(" << sync_slave_partitions_.size() << "):" << "\r\n";
  for (auto& iter : sync_slave_partitions_) {
    tmp_stream << " Partition " << iter.second->SyncPartitionInfo().ToString()
      << "\r\n" << iter.second->ToStringStatus() << "\r\n";
  }
  info->append(tmp_stream.str());
}
