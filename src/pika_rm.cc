// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "set"

#include "include/pika_rm.h"
#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_repl_client.h"
#include "include/pika_repl_server.h"

#include <glog/logging.h>

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
    const std::string& table_name, uint32_t partition_id)
  : RmNode(ip, port, PartitionInfo(table_name, partition_id)),
  slave_state(kSlaveNotSync),
  b_state(kNotSync), sent_offset(), acked_offset() {
}

SlaveNode::~SlaveNode() {
  if (b_state == kReadFromFile && binlog_reader != nullptr) {
    RmNode rm_node(Ip(), Port(), TableName(), PartitionId());
    ReleaseBinlogFileReader();
  }
}

Status SlaveNode::InitBinlogFileReader(const std::shared_ptr<Binlog>& binlog, const BinlogOffset& offset) {
  Status s = g_pika_rm->binlog_reader_mgr.FetchBinlogReader(RmNode(Ip(), Port(), NodePartitionInfo()), &binlog_reader);
  if (!s.ok()) {
    return s;
  }
  int res = binlog_reader->Seek(binlog, offset.filenum, offset.offset);
  if (res) {
    g_pika_rm->binlog_reader_mgr.ReleaseBinlogReader(RmNode(Ip(), Port(), NodePartitionInfo()));
    return Status::Corruption(ToString() + "  binlog reader init failed");
  }
  return Status::OK();
}

void SlaveNode::ReleaseBinlogFileReader() {
  g_pika_rm->binlog_reader_mgr.ReleaseBinlogReader(RmNode(Ip(), Port(), NodePartitionInfo()));
  binlog_reader = nullptr;
}

/* SyncPartition */

SyncPartition::SyncPartition(const std::string& table_name, uint32_t partition_id)
  : partition_info_(table_name, partition_id) {
}

/* SyncMasterPartition*/

SyncMasterPartition::SyncMasterPartition(const std::string& table_name, uint32_t partition_id) : SyncPartition(table_name, partition_id) {
}

bool SyncMasterPartition::CheckReadBinlogFromCache() {
  return false;
}

Status SyncMasterPartition::AddSlaveNode(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave : slaves_) {
    if (ip == slave->Ip() && port == slave->Port()) {
      return Status::OK();
    }
  }
  std::shared_ptr<SlaveNode> slave_ptr = std::make_shared<SlaveNode>(ip, port, SyncPartitionInfo().table_name_, SyncPartitionInfo().partition_id_);
  slave_ptr->SetLastSendTime(slash::NowMicros());
  slave_ptr->SetLastRecvTime(slash::NowMicros());
  slaves_.push_back(slave_ptr);
  LOG(INFO) << "Add Slave Node Partition " << SyncPartitionInfo().ToString() << ", Master AddSlaveNode"<< ip << ":" << port;
  return Status::OK();
}

Status SyncMasterPartition::RemoveSlaveNode(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (size_t i  = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> slave = slaves_[i];
    if (ip == slave->Ip() && port == slave->Port()) {
      slaves_.erase(slaves_.begin() + i);
      LOG(INFO) << "Remove Slave Node Partiiton " << SyncPartitionInfo().ToString() << ", Master RemoveSlaveNode "<< ip << ":" << port;
      return Status::OK();
    }
  }
  return Status::NotFound("RemoveSlaveNode" + ip + std::to_string(port));
}

Status SyncMasterPartition::ActivateSlaveBinlogSync(const std::string& ip, int port, const std::shared_ptr<Binlog> binlog, const BinlogOffset& offset) {
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
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->NodePartitionInfo());
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

Status SyncMasterPartition::GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset, BinlogOffset* acked_offset) {
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
      RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->TableName(), slave_ptr->PartitionId());
      WriteTask empty_task(rm_node, BinlogChip(BinlogOffset(0, 0), ""));
      task.push_back(empty_task);
      Status s = g_pika_rm->GetPikaReplServer()->SendSlaveBinlogChips(slave_ptr->Ip(), slave_ptr->Port(), task);
      slave_ptr->SetLastSendTime(now);
      if (!s.ok()) {
        return Status::Corruption("Send ping failed " + slave_ptr->Ip() + std::to_string(slave_ptr->Port()));
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

/* SyncSlavePartition */

SyncSlavePartition::SyncSlavePartition(const std::string& table_name, uint32_t partition_id, const RmNode& master) : SyncPartition(table_name, partition_id), m_info_(master), repl_state_(kNoConnect) {
  m_info_.SetLastRecvTime(slash::NowMicros());
}

Status SyncSlavePartition::SetLastRecvTime(uint64_t time) {
  slash::MutexLock l(&partition_mu_);
  m_info_.SetLastRecvTime(time);
  return Status::OK();
}

Status SyncSlavePartition::GetLastRecvTime(uint64_t* time) {
  slash::MutexLock l(&partition_mu_);
  *time = m_info_.LastRecvTime();
  return Status::OK();
}

Status SyncSlavePartition::CheckSyncTimeout(uint64_t now, bool* del) {
  slash::MutexLock l(&partition_mu_);

  if (m_info_.LastRecvTime() + kRecvKeepAliveTimeout < now) {
    std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(
          m_info_.TableName(),
          m_info_.PartitionId());
    if (!partition) {
      LOG(WARNING) << "Partition: " << m_info_.TableName()<< ":" <<
        m_info_.PartitionId() << " Not Found";
    }
    partition->SetReplState(ReplState::kTryConnect);
    *del = true;
  }
  return Status::OK();
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

PikaReplicaManager::PikaReplicaManager() {
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

PikaReplClient* PikaReplicaManager::GetPikaReplClient() {
  return pika_repl_client_;
}

PikaReplServer* PikaReplicaManager::GetPikaReplServer() {
  return pika_repl_server_;
}

void PikaReplicaManager::InitPartition() {
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  for (const auto& table : table_structs) {
    const std::string& table_name = table.table_name;
    uint32_t partition_num = table.partition_num;
    for (uint32_t index = 0; index < partition_num; ++index) {
      sync_master_partitions_[PartitionInfo(table_name, index)]
        = std::make_shared<SyncMasterPartition>(table_name, index);
    }
  }
}

Status PikaReplicaManager::RebuildPartition() {
  slash::RWLock l(&partitions_rw_, true);
  if (!sync_slave_partitions_.empty()) {
    return Status::Corruption("Slave partition is NOT empty");
  }
  sync_master_partitions_.clear();
  InitPartition();
  LOG(INFO) << "Rebuild Sync Partition Success! " <<
    "Rebuilded partition size " << sync_master_partitions_.size();
  return Status::OK();
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
  s = partition->AddSlaveNode(slave.Ip(), slave.Port());
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
  slash::RWLock l_part(&partitions_rw_, true);
  for (auto& iter : sync_master_partitions_) {
    std::shared_ptr<SyncMasterPartition> partition = iter.second;
    Status s = partition->RemoveSlaveNode(ip, port);
    if (!s.ok() && !s.IsNotFound()) {
      LOG(WARNING) << "Lost Connection failed " << s.ToString();
    }
  }

  std::vector<PartitionInfo> to_del;
  for (auto& iter : sync_slave_partitions_) {
    std::shared_ptr<SyncSlavePartition> partition = iter.second;
    if (partition->MasterIp() == ip && partition->MasterPort() == port) {
      LOG(INFO) << "Slave Del slave partition " << partition->SyncPartitionInfo().ToString()
        << " master " << partition->MasterIp() << " " << partition->MasterPort();
      to_del.push_back(partition->SyncPartitionInfo());
    }
  }
  for (auto& partition_info : to_del) {
    sync_slave_partitions_.erase(partition_info);
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


Status PikaReplicaManager::AddSyncMasterPartition(const std::string& table_name, uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, true);
  PartitionInfo partition_info(table_name, partition_id);
  auto master_iter = sync_master_partitions_.find(partition_info);
  if (master_iter != sync_master_partitions_.end()) {
    return Status::OK();
  }
  sync_master_partitions_[partition_info] =
    std::make_shared<SyncMasterPartition>(table_name, partition_id);
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncMasterPartition(const std::string& table_name, uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, true);
  PartitionInfo partition_info(table_name, partition_id);
  auto master_iter = sync_master_partitions_.find(partition_info);
  if (master_iter == sync_master_partitions_.end()) {
    return Status::NotFound(table_name + std::to_string(partition_id));
  }
  sync_master_partitions_.erase(master_iter);
  return Status::OK();
}

Status PikaReplicaManager::AddSyncSlavePartition(const RmNode& node) {
  slash::RWLock l(&partitions_rw_, true);
  const PartitionInfo& partition_info = node.NodePartitionInfo();
  auto slave_iter = sync_slave_partitions_.find(partition_info);
  if (slave_iter != sync_slave_partitions_.end()) {
    return Status::OK();
  }
  sync_slave_partitions_[partition_info] =
    std::make_shared<SyncSlavePartition>(node.TableName(), node.PartitionId(), node);
  LOG(INFO) << "Slave Add salve partition " << partition_info.ToString() <<
    " master " << node.Ip() << " " << node.Port();
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncSlavePartition(const RmNode& node) {
  slash::RWLock l(&partitions_rw_, true);
  const PartitionInfo& partition_info = node.NodePartitionInfo();
  auto slave_iter = sync_slave_partitions_.find(partition_info);
  if (slave_iter == sync_slave_partitions_.end()) {
    return Status::NotFound(node.ToString());
  }
  sync_slave_partitions_.erase(slave_iter);
  LOG(INFO) <<" Slave del slave partition success " << partition_info.ToString();
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
  slash::RWLock l(&partitions_rw_, true);

  for (auto& iter : sync_master_partitions_) {
    std::shared_ptr<SyncMasterPartition> partition = iter.second;
    Status s = partition->CheckSyncTimeout(now);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
  }
  std::vector<PartitionInfo> to_del;
  for (auto& iter : sync_slave_partitions_) {
    std::shared_ptr<SyncSlavePartition> partition = iter.second;
    bool del = false;
    Status s = partition->CheckSyncTimeout(now, &del);
    if (!s.ok()) {
      LOG(WARNING) << "CheckSyncTimeout Failed " << s.ToString();
    }
    if (del) {
      to_del.push_back(PartitionInfo(partition->SyncPartitionInfo()));
    }
  }
  for (auto& partition_info : to_del) {
    sync_slave_partitions_.erase(partition_info);
    LOG(INFO) <<" Slave del slave partition success " << partition_info.ToString();
  }
  return Status::OK();
}
