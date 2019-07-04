// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "set"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <glog/logging.h>

#include "pink/include/pink_cli.h"

#include "include/pika_rm.h"
#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"


extern PikaConf *g_pika_conf;
extern PikaReplicaManager* g_pika_rm;
extern PikaServer *g_pika_server;

/* BinlogReaderManager */

BinlogReaderManager::~BinlogReaderManager() {
}

Status BinlogReaderManager::FetchBinlogReader(const RmNode& rm_node, std::shared_ptr<PikaBinlogReader>* reader) {
  slash::MutexLock l(&reader_mu_);
  if (occupied_.find(rm_node) != occupied_.end()) {
    return Status::Corruption(rm_node.ToString() + " exist");
  }
  if (vacant_.empty()) {
    *reader = std::make_shared<PikaBinlogReader>();
  } else {
    *reader = *(vacant_.begin());
    vacant_.erase(vacant_.begin());
  }
  occupied_[rm_node] = *reader;
  return Status::OK();
}

Status BinlogReaderManager::ReleaseBinlogReader(const RmNode& rm_node) {
  slash::MutexLock l(&reader_mu_);
  if (occupied_.find(rm_node) == occupied_.end()) {
    return Status::NotFound(rm_node.ToString());
  }
  std::shared_ptr<PikaBinlogReader> reader = occupied_[rm_node];
  occupied_.erase(rm_node);
  vacant_.push_back(reader);
  return Status::OK();
}

/* SlaveNode */

SlaveNode::SlaveNode(const std::string& ip, int port,
                     const std::string& table_name,
                     uint32_t partition_id, int session_id)
  : RmNode(ip, port, table_name, partition_id, session_id),
  slave_state(kSlaveNotSync),
  b_state(kNotSync), sent_offset(), acked_offset() {
}

SlaveNode::~SlaveNode() {
  if (b_state == kReadFromFile && binlog_reader != nullptr) {
    RmNode rm_node(Ip(), Port(), TableName(), PartitionId());
    ReleaseBinlogFileReader();
  }
}

Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog,
                                       const BinlogOffset& offset) {
  Status s = g_pika_rm->binlog_reader_mgr.FetchBinlogReader(
      RmNode(Ip(), Port(), NodePartitionInfo()), &binlog_reader);
  if (!s.ok()) {
    return s;
  }
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res) {
    g_pika_rm->binlog_reader_mgr.ReleaseBinlogReader(
        RmNode(Ip(), Port(), NodePartitionInfo()));
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

void SlaveNode::ReleaseBinlogFileReader() {
  g_pika_rm->binlog_reader_mgr.ReleaseBinlogReader(
      RmNode(Ip(), Port(), NodePartitionInfo()));
  binlog_reader = nullptr;
}

std::string SlaveNode::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << "    Slave_state: " << SlaveStateMsg[slave_state] << "\r\n";
  tmp_stream << "    Binlog_sync_state: " << BinlogSyncStateMsg[b_state] << "\r\n";
  tmp_stream << "    Sync_window: " << "\r\n" << sync_win.ToStringStatus();
  tmp_stream << "    Sent_offset: " << sent_offset.ToString() << "\r\n";
  tmp_stream << "    Acked_offset: " << acked_offset.ToString() << "\r\n";
  tmp_stream << "    Binlog_reader activated: " << (binlog_reader != nullptr) << "\r\n";
  return tmp_stream.str();
}

/* SyncPartition */

SyncPartition::SyncPartition(const std::string& table_name, uint32_t partition_id)
  : partition_info_(table_name, partition_id) {
}

/* SyncMasterPartition*/

SyncMasterPartition::SyncMasterPartition(const std::string& table_name, uint32_t partition_id)
    : SyncPartition(table_name, partition_id),
      session_id_(0) {}

bool SyncMasterPartition::CheckReadBinlogFromCache() {
  return false;
}

int SyncMasterPartition::GetNumberOfSlaveNode() {
  slash::MutexLock l(&partition_mu_);
  return slaves_.size();
}

bool SyncMasterPartition::CheckSlaveNodeExist(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave : slaves_) {
    if (ip == slave->Ip() && port == slave->Port()) {
      return true;
    }
  }
  return false;
}

Status SyncMasterPartition::GetSlaveNodeSession(
    const std::string& ip, int port, int32_t* session) {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave : slaves_) {
    if (ip == slave->Ip() && port == slave->Port()) {
      *session = slave->SessionId();
      return Status::OK();
    }
  }
  return Status::NotFound("slave " + ip + ":" + std::to_string(port) + " not found");
}

Status SyncMasterPartition::AddSlaveNode(const std::string& ip, int port, int session_id) {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave : slaves_) {
    if (ip == slave->Ip() && port == slave->Port()) {
      slave->SetSessionId(session_id);
      return Status::OK();
    }
  }
  std::shared_ptr<SlaveNode> slave_ptr =
    std::make_shared<SlaveNode>(ip, port, SyncPartitionInfo().table_name_, SyncPartitionInfo().partition_id_, session_id);
  slave_ptr->SetLastSendTime(slash::NowMicros());
  slave_ptr->SetLastRecvTime(slash::NowMicros());
  slaves_.push_back(slave_ptr);
  LOG(INFO) << "Add Slave Node, partition: " << SyncPartitionInfo().ToString() << ", ip_port: "<< ip << ":" << port;
  return Status::OK();
}

Status SyncMasterPartition::RemoveSlaveNode(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (size_t i  = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> slave = slaves_[i];
    if (ip == slave->Ip() && port == slave->Port()) {
      slaves_.erase(slaves_.begin() + i);
      LOG(INFO) << "Remove Slave Node, Partition: " << SyncPartitionInfo().ToString()
        << ", ip_port: "<< ip << ":" << port;
      return Status::OK();
    }
  }
  return Status::NotFound("RemoveSlaveNode" + ip + std::to_string(port));
}

Status SyncMasterPartition::ActivateSlaveBinlogSync(const std::string& ip,
                                                    int port,
                                                    const std::shared_ptr<Binlog> binlog,
                                                    const BinlogOffset& offset) {
  {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }
  bool read_cache = CheckReadBinlogFromCache();

  slave_ptr->Lock();
  slave_ptr->slave_state = kSlaveBinlogSync;
  slave_ptr->sent_offset = offset;
  slave_ptr->acked_offset = offset;
  if (read_cache) {
    slave_ptr->Unlock();
    // RegistToBinlogCacheWindow(ip, port, offset);
    slave_ptr->Lock();
    slave_ptr->b_state = kReadFromCache;
  } else {
    // read binlog file from file
    s = slave_ptr->InitBinlogFileReader(binlog, offset);
    if (!s.ok()) {
      slave_ptr->Unlock();
      return Status::Corruption("Init binlog file reader failed" + s.ToString());
    }
    slave_ptr->b_state = kReadFromFile;
  }
  slave_ptr->Unlock();
  }

  Status s = SyncBinlogToWq(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status SyncMasterPartition::SyncBinlogToWq(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  if (slave_ptr->b_state == kReadFromFile) {
    ReadBinlogFileToWq(slave_ptr);
  } else if (slave_ptr->b_state == kReadFromCache) {
    ReadCachedBinlogToWq(slave_ptr);
  }
  }
  return Status::OK();
}

Status SyncMasterPartition::ActivateSlaveDbSync(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  slave_ptr->slave_state = kSlaveDbSync;
  // invoke db sync
  }
  return Status::OK();
}

Status SyncMasterPartition::ReadCachedBinlogToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
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

Status SyncMasterPartition::GetSlaveNode(const std::string& ip, int port, std::shared_ptr<SlaveNode>* slave_node) {
  for (size_t i  = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> tmp_slave = slaves_[i];
    if (ip == tmp_slave->Ip() && port == tmp_slave->Port()) {
      *slave_node = tmp_slave;
      return Status::OK();
    }
  }
  return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
}

Status SyncMasterPartition::UpdateSlaveBinlogAckInfo(const std::string& ip, int port, const BinlogOffset& start, const BinlogOffset& end) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  if (slave_ptr->slave_state != kSlaveBinlogSync) {
    return Status::Corruption(ip + std::to_string(port) + "state not BinlogSync");
  }
  bool res = slave_ptr->sync_win.Update(SyncWinItem(start), SyncWinItem(end), &(slave_ptr->acked_offset));
  if (!res) {
    return Status::Corruption("UpdateAckedInfo failed");
  }
  }
  return Status::OK();
}

Status SyncMasterPartition::GetSlaveSyncBinlogInfo(const std::string& ip,
                                                   int port,
                                                   BinlogOffset* sent_offset,
                                                   BinlogOffset* acked_offset) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  *sent_offset = slave_ptr->sent_offset;
  *acked_offset = slave_ptr->acked_offset;
  }
  return Status::OK();
}

Status SyncMasterPartition::GetSlaveState(const std::string& ip,
                                          int port,
                                          SlaveState* const slave_state) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  *slave_state = slave_ptr->slave_state;
  }
  return Status::OK();
}

Status SyncMasterPartition::WakeUpSlaveBinlogSync() {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave_ptr : slaves_) {
    {
    slash::MutexLock l(&slave_ptr->slave_mu);
    if (slave_ptr->sent_offset == slave_ptr->acked_offset) {
      if (slave_ptr->b_state == kReadFromFile) {
        ReadBinlogFileToWq(slave_ptr);
      } else if (slave_ptr->b_state == kReadFromCache) {
        ReadCachedBinlogToWq(slave_ptr);
      }
    }
    }
  }
  return Status::OK();
}

Status SyncMasterPartition::SetLastSendTime(const std::string& ip, int port, uint64_t time) {
  slash::MutexLock l(&partition_mu_);

  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  slave_ptr->SetLastSendTime(time);
  }

  return Status::OK();
}

Status SyncMasterPartition::GetLastSendTime(const std::string& ip, int port, uint64_t* time) {
  slash::MutexLock l(&partition_mu_);

  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  *time = slave_ptr->LastSendTime();
  }

  return Status::OK();
}

Status SyncMasterPartition::SetLastRecvTime(const std::string& ip, int port, uint64_t time) {
  slash::MutexLock l(&partition_mu_);

  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  slave_ptr->SetLastRecvTime(time);
  }

  return Status::OK();
}

Status SyncMasterPartition::GetLastRecvTime(const std::string& ip, int port, uint64_t* time) {
  slash::MutexLock l(&partition_mu_);

  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  *time = slave_ptr->LastRecvTime();
  }

  return Status::OK();
}

Status SyncMasterPartition::CheckSyncTimeout(uint64_t now) {
  slash::MutexLock l(&partition_mu_);

  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  std::vector<Node> to_del;

  for (auto& slave_ptr : slaves_) {
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
    for (size_t i = 0; i < slaves_.size(); ++i) {
      LOG(INFO)<< SyncPartitionInfo().ToString() << " slave " << slaves_[i]->ToString();
      if (node.Ip() == slaves_[i]->Ip() && node.Port() == slaves_[i]->Port()) {
        slaves_.erase(slaves_.begin() + i);
        LOG(INFO) << SyncPartitionInfo().ToString() << " Master del slave success " << node.ToString();
        break;
      }
    }
  }
  return Status::OK();
}

std::string SyncMasterPartition::ToStringStatus() {
  std::stringstream tmp_stream;
  tmp_stream << " Current Master Session: " << session_id_ << "\r\n";
  slash::MutexLock l(&partition_mu_);
  for (size_t i = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> slave_ptr = slaves_[i];
    slash::MutexLock l(&slave_ptr->slave_mu);
    tmp_stream << "  slave[" << i << "]: "  << slave_ptr->ToString() <<
      "\r\n" << slave_ptr->ToStringStatus();
  }
  return tmp_stream.str();
}

int32_t SyncMasterPartition::GenSessionId() {
  slash::MutexLock ml(&session_mu_);
  return session_id_++;
}

bool SyncMasterPartition::CheckSessionId(const std::string& ip, int port,
                                         const std::string& table_name,
                                         uint64_t partition_id, int session_id) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    LOG(WARNING)<< "Check SessionId Get Slave Node Error: "
        << ip << ":" << port << "," << table_name << "_" << partition_id;
    return false;
  }
  if (session_id != slave_ptr->SessionId()) {
    LOG(WARNING)<< "Check SessionId Mismatch: " << ip << ":" << port << ", "
        << table_name << "_" << partition_id << " expected_session: " << session_id
        << ", actual_session:" << slave_ptr->SessionId();
    return false;
  }
  return true;
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

/* SyncWindow */

void SyncWindow::Push(const SyncWinItem& item) {
  win_.push_back(item);
}

bool SyncWindow::Update(const SyncWinItem& start_item, const SyncWinItem& end_item, BinlogOffset* acked_offset) {
  size_t start_pos = kBinlogReadWinSize, end_pos = kBinlogReadWinSize;
  for (size_t i = 0; i < win_.size(); ++i) {
    if (win_[i] == start_item) {
      start_pos = i;
    }
    if (win_[i] == end_item) {
      end_pos = i;
      break;
    }
  }
  if (start_pos == kBinlogReadWinSize || end_pos == kBinlogReadWinSize) {
    LOG(WARNING) << " ack offset not found in binlog controller window";
    return false;
  }
  for (size_t i = start_pos; i <= end_pos; ++i) {
    win_[i].acked_ = true;
  }
  while (!win_.empty()) {
    if (win_[0].acked_) {
      *acked_offset = win_[0].offset_;
      win_.erase(win_.begin());
    } else {
      break;
    }
  }
  return true;
}

int SyncWindow::Remainings() {
  return kBinlogReadWinSize - win_.size();
}

/* PikaReplicaManger */

PikaReplicaManager::PikaReplicaManager()
    : last_meta_sync_timestamp_(0){
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
  int counter = 0;
  slash::MutexLock l(&write_queue_mu_);
  std::vector<std::string> to_delete;
  for (auto& iter : write_queues_) {
    std::queue<WriteTask>& queue = iter.second;
    for (int i = 0; i < kBinlogSendPacketNum; ++i) {
      if (queue.empty()) {
        break;
      }
      size_t batch_index = queue.size() > kBinlogSendBatchNum ? kBinlogSendBatchNum : queue.size();
      std::string ip;
      int port = 0;
      if (!slash::ParseIpPortString(iter.first, ip, port)) {
        LOG(WARNING) << "Parse ip_port error " << iter.first;
        continue;
      }
      std::vector<WriteTask> to_send;
      for (size_t i = 0; i < batch_index; ++i) {
        to_send.push_back(queue.front());
        queue.pop();
        counter++;
      }
      Status s = pika_repl_server_->SendSlaveBinlogChips(ip, port, to_send);
      if (!s.ok()) {
        LOG(WARNING) << "send binlog to " << ip << ":" << port << " failed, " << s.ToString();
        to_delete.push_back(iter.first);
        break;
      }
    }
  }
  for (auto& del_queue : to_delete) {
    write_queues_.erase(del_queue);
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
  Status s = partition->UpdateSlaveBinlogAckInfo(slave.Ip(), slave.Port(), range_start, range_end);
  if (!s.ok()) {
    return s;
  }
  s = partition->SyncBinlogToWq(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::GetSyncBinlogStatus(const RmNode& slave, BinlogOffset*  sent_offset, BinlogOffset* acked_offset) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s = partition->GetSlaveSyncBinlogInfo(slave.Ip(), slave.Port(), sent_offset, acked_offset);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::GetSyncMasterPartitionSlaveState(const RmNode& slave,
                                                            SlaveState* const slave_state) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s = partition->GetSlaveState(slave.Ip(), slave.Port(), slave_state);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

bool PikaReplicaManager::CheckPartitionSlaveExist(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return false;
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  return partition->CheckSlaveNodeExist(slave.Ip(), slave.Port());
}

Status PikaReplicaManager::GetPartitionSlaveSession(const RmNode& slave, int32_t* session) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString(), + "not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  return partition->GetSlaveNodeSession(slave.Ip(), slave.Port(), session);
}

Status PikaReplicaManager::AddPartitionSlave(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s= partition->RemoveSlaveNode(slave.Ip(), slave.Port());
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  s = partition->AddSlaveNode(slave.Ip(), slave.Port(), slave.SessionId());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::RemovePartitionSlave(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s = partition->RemoveSlaveNode(slave.Ip(), slave.Port());
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

Status PikaReplicaManager::ActivateBinlogSync(const RmNode& slave, const BinlogOffset& offset) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> sync_partition = sync_master_partitions_[slave.NodePartitionInfo()];

  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(slave.TableName(), slave.PartitionId());
  if (!partition) {
    return Status::Corruption("Found Binlog faile");
  }

  Status s = sync_partition->ActivateSlaveBinlogSync(slave.Ip(), slave.Port(), partition->logger(), offset);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::ActivateDbSync(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(slave.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[slave.NodePartitionInfo()];
  Status s = partition->ActivateSlaveDbSync(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::SetMasterLastRecvTime(const RmNode& node, uint64_t time) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_master_partitions_.find(node.NodePartitionInfo()) == sync_master_partitions_.end()) {
    return Status::NotFound(node.ToString() + " not found");
  }
  std::shared_ptr<SyncMasterPartition> partition = sync_master_partitions_[node.NodePartitionInfo()];
  partition->SetLastRecvTime(node.Ip(), node.Port(), time);
  return Status::OK();
}

Status PikaReplicaManager::SetSlaveLastRecvTime(const RmNode& node, uint64_t time) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(node.NodePartitionInfo()) == sync_slave_partitions_.end()) {
    return Status::NotFound(node.ToString() + " not found");
  }
  std::shared_ptr<SyncSlavePartition> partition = sync_slave_partitions_[node.NodePartitionInfo()];
  partition->SetLastRecvTime(time);
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

int32_t PikaReplicaManager::GenPartitionSessionId(const std::string& table_name,
                                                  uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table_name, partition_id);
  if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()) {
    return -1;
  } else {
    std::shared_ptr<SyncMasterPartition> sync_master_partition = sync_master_partitions_[p_info];
    return sync_master_partition->GenSessionId();
  }
}

int32_t PikaReplicaManager::GetSlavePartitionSessionId(const std::string& table_name,
                                                       uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table_name, partition_id);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return -1;
  } else {
    std::shared_ptr<SyncSlavePartition> sync_slave_partition = sync_slave_partitions_[p_info];
    return sync_slave_partition->MasterSessionId();
  }
}

bool PikaReplicaManager::CheckSlavePartitionSessionId(const std::string& table_name,
                                                      uint32_t partition_id,
                                                      int session_id) {
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table_name, partition_id);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    LOG(WARNING)<< "Slave Partition Not Found: " << p_info.ToString().data();
    return false;
  } else {
    std::shared_ptr<SyncSlavePartition> sync_slave_partition = sync_slave_partitions_[p_info];
    if (sync_slave_partition->MasterSessionId() != session_id) {
      LOG(WARNING)<< "Check SessionId Mismatch: " << sync_slave_partition->MasterIp()
        << ":" << sync_slave_partition->MasterPort() << ", "
        << sync_slave_partition->SyncPartitionInfo().ToString()
        << " expected_session: " << session_id << ", actual_session:"
        << sync_slave_partition->MasterSessionId();
      return false;
    }
  }
  return true;
}

bool PikaReplicaManager::CheckMasterPartitionSessionId(const std::string& ip, int port,
                                                       const std::string& table_name,
                                                       uint32_t partition_id, int session_id) {
  slash::RWLock l(&partitions_rw_, false);
  PartitionInfo p_info(table_name, partition_id);
  if (sync_master_partitions_.find(p_info) == sync_master_partitions_.end()) {
    return false;
  } else {
    std::shared_ptr<SyncMasterPartition> sync_master_partition = sync_master_partitions_[p_info];
    return sync_master_partition->CheckSessionId(ip, port, table_name, partition_id, session_id);
  }
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
    return Status::Corruption("Connect remote node error");
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

Status PikaReplicaManager::UpdateSyncSlavePartitionSessionId(const PartitionInfo& p_info,
                                                             int32_t session_id) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  }
  sync_slave_partitions_[p_info]->SetMasterSessionId(session_id);
  return Status::OK();
}

Status PikaReplicaManager::DeactivateSyncSlavePartition(const PartitionInfo& p_info) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  }
  sync_slave_partitions_[p_info]->Deactivate();
  return Status::OK();
}

Status PikaReplicaManager::SetSlaveReplState(const PartitionInfo& p_info,
                                             const ReplState& repl_state) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  }
  sync_slave_partitions_[p_info]->SetReplState(repl_state);
  return Status::OK();
}

Status PikaReplicaManager::GetSlaveReplState(const PartitionInfo& p_info,
                                             ReplState* repl_state) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return Status::NotFound("Sync Slave partition " + p_info.ToString());
  }
  *repl_state = sync_slave_partitions_[p_info]->State();
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
  std::shared_ptr<SyncSlavePartition> slave_partition =
      GetSyncSlavePartitionByName(PartitionInfo(table, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table << ":" << partition_id
        << ", NotFound";
    return Status::Corruption("Slave Partition not found");
  }
  return pika_repl_client_->SendRemoveSlaveNode(
          slave_partition->MasterIp(), slave_partition->MasterPort(),
          table, partition_id, slave_partition->LocalIp());
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
    s = g_pika_rm->SetSlaveReplState(PartitionInfo(table_name, partition_id), ReplState::kWaitReply);
  } else {
    s = g_pika_rm->SetSlaveReplState(PartitionInfo(table_name, partition_id), ReplState::kError);
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
    s = g_pika_rm->SetSlaveReplState(PartitionInfo(table_name, partition_id), ReplState::kWaitReply);
  } else {
    LOG(WARNING) << "SendPartitionDbSync failed " << status.ToString();
    s = g_pika_rm->SetSlaveReplState(PartitionInfo(table_name, partition_id), ReplState::kError);
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

std::shared_ptr<SyncSlavePartition>
PikaReplicaManager::GetSyncSlavePartitionByName(const PartitionInfo& p_info) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_slave_partitions_.find(p_info) == sync_slave_partitions_.end()) {
    return nullptr;
  }
  return sync_slave_partitions_[p_info];
}

Status PikaReplicaManager::AddSyncPartition(
        const std::set<PartitionInfo>& p_infos) {
  slash::RWLock l(&partitions_rw_, true);
  for (const auto& p_info : p_infos) {
    if (sync_master_partitions_.find(p_info) != sync_master_partitions_.end()
      || sync_slave_partitions_.find(p_info) != sync_slave_partitions_.end()) {
      LOG(WARNING) << "sync partition: " << p_info.ToString() << " exist";
      return Status::Corruption("sync partition " + p_info.ToString()
          + " exist");
    }
  }

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

Status PikaReplicaManager::RemoveSyncPartition(
        const std::set<PartitionInfo>& p_infos) {
  slash::RWLock l(&partitions_rw_, true);
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

  for (const auto& p_info : p_infos) {
    sync_master_partitions_.erase(p_info);
    sync_slave_partitions_.erase(p_info);
  }
  return Status::OK();
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
