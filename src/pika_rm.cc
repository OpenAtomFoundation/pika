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
  last_active_time(0),
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


/* MasterNode */

MasterNode::MasterNode(const std::string& ip, int port, const std::string& table_name, uint32_t partition_id) : RmNode(ip, port, PartitionInfo(table_name, partition_id)) {
}

MasterNode::MasterNode() : RmNode() {
}

/* SyncPartition */

SyncPartition::SyncPartition(const std::string& table_name, uint32_t partition_id)
  : partition_info_(table_name, partition_id), role_(kPartitionSingle), m_info_() {
  repl_state_ = kNoConnect;
}


bool SyncPartition::CheckReadBinlogFromCache() {
  return false;
}

void SyncPartition::CleanMasterNode() {
  m_info_ = MasterNode();
  repl_state_ = kNoConnect;
}

void SyncPartition::CleanSlaveNode() {
  slaves_.clear();
}

void SyncPartition::BecomeSingle() {
  slash::MutexLock l(&partition_mu_);
  if (role_ == kPartitionSingle) {
    return;
  }

  CleanMasterNode();
  CleanSlaveNode();

  role_ = kPartitionSingle;
}

void SyncPartition::BecomeMaster()  {
  slash::MutexLock l(&partition_mu_);
  if (role_ == kPartitionMaster) {
    return;
  }

  CleanMasterNode();

  role_ = kPartitionMaster;
}

void SyncPartition::BecomeSlave(const std::string& master_ip, int master_port) {
  slash::MutexLock l(&partition_mu_);

  CleanSlaveNode();

  m_info_ = MasterNode(master_ip, master_port, partition_info_.table_name_, partition_info_.partition_id_);
  repl_state_ = kNoConnect;

  role_ = kPartitionSlave;
}

Status SyncPartition::AddSlaveNode(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (auto& slave : slaves_) {
    if (ip == slave->Ip() && port == slave->Port()) {
      return Status::OK();
    }
  }
  slaves_.push_back(std::make_shared<SlaveNode>(ip, port, partition_info_.table_name_, partition_info_.partition_id_));
  return Status::OK();
}

Status SyncPartition::RemoveSlaveNode(const std::string& ip, int port) {
  slash::MutexLock l(&partition_mu_);
  for (size_t i  = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> slave = slaves_[i];
    if (ip == slave->Ip() && port == slave->Port()) {
      slaves_.erase(slaves_.begin() + i);
      return Status::OK();
    }
  }
  return Status::NotFound("RemoveSlaveNode" + ip + std::to_string(port));
}

Status SyncPartition::ActivateSlaveBinlogSync(const std::string& ip, int port, const std::shared_ptr<Binlog> binlog, const BinlogOffset& offset) {
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

Status SyncPartition::SyncBinlogToWq(const std::string& ip, int port) {
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

Status SyncPartition::ActivateSlaveDbSync(const std::string& ip, int port) {
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

Status SyncPartition::ReadCachedBinlogToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
  return Status::OK();
}

Status SyncPartition::ReadBinlogFileToWq(const std::shared_ptr<SlaveNode>& slave_ptr) {
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
      LOG(WARNING) << partition_info_.ToString()
        << " Read Binlog error : " << s.ToString();
      return s;
    }
    slave_ptr->sync_win.Push(SyncWinItem(filenum, offset));

    BinlogOffset sent_offset = BinlogOffset(filenum, offset);
    slave_ptr->sent_offset = sent_offset;
    RmNode rm_node(slave_ptr->Ip(), slave_ptr->Port(), slave_ptr->NodePartitionInfo());
    WriteTask task(rm_node, BinlogChip(sent_offset, msg));
    tasks.push_back(task);
  }
  if (!tasks.empty()) {
    g_pika_rm->ProduceWriteQueue(slave_ptr->Ip(), slave_ptr->Port(), tasks);
  }
  return Status::OK();
}

Status SyncPartition::GetSlaveNode(const std::string& ip, int port, std::shared_ptr<SlaveNode>* slave_node) {
  for (size_t i  = 0; i < slaves_.size(); ++i) {
    std::shared_ptr<SlaveNode> tmp_slave = slaves_[i];
    if (ip == tmp_slave->Ip() && port == tmp_slave->Port()) {
      *slave_node = tmp_slave;
      return Status::OK();
    }
  }
  return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
}

Status SyncPartition::UpdateSlaveBinlogAckInfo(const std::string& ip, int port, const BinlogOffset& start, const BinlogOffset& end) {
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

Status SyncPartition::GetSlaveSyncBinlogInfo(const std::string& ip, int port, BinlogOffset* sent_offset, BinlogOffset* acked_offset) {
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

Status SyncPartition::WakeUpSlaveBinlogSync() {
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

Status SyncPartition::SetLastActiveTime(const std::string& ip, int port, uint64_t time) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  slave_ptr->last_active_time = time;
  }
  return Status::OK();
}

Status SyncPartition::GetLastActiveTime(const std::string& ip, int port, uint64_t* time) {
  slash::MutexLock l(&partition_mu_);
  std::shared_ptr<SlaveNode> slave_ptr = nullptr;
  Status s = GetSlaveNode(ip, port, &slave_ptr);
  if (!s.ok()) {
    return s;
  }

  {
  slash::MutexLock l(&slave_ptr->slave_mu);
  *time = slave_ptr->last_active_time;
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
      sync_partitions_[PartitionInfo(table_name, index)]
        = std::make_shared<SyncPartition>(table_name, index);
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
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
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
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  Status s = partition->GetSlaveSyncBinlogInfo(slave.Ip(), slave.Port(), sent_offset, acked_offset);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSyncPartition(const std::string& table_name, uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, true);
  PartitionInfo partition_info(table_name, partition_id);
  if (sync_partitions_.find(partition_info) == sync_partitions_.end()) {
    return Status::Corruption(table_name + std::to_string(partition_id) + " already exist");
  }
  sync_partitions_[partition_info] = std::make_shared<SyncPartition>(table_name, partition_id);
  return Status::OK();
}

Status PikaReplicaManager::RemoveSyncPartition(const std::string& table_name, uint32_t partition_id) {
  slash::RWLock l(&partitions_rw_, true);
  PartitionInfo partition_info(table_name, partition_id);
  if (sync_partitions_.find(partition_info) == sync_partitions_.end()) {
    return Status::NotFound(table_name + std::to_string(partition_id));
  }
  sync_partitions_.erase(partition_info);
  return Status::OK();
}

Status PikaReplicaManager::AddPartitionSlave(const RmNode& slave) {
  Status s = AddSlave(slave);
  if (!s.ok()) {
    return s;
  }
  s = RecordNodePartition(slave);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::AddSlave(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  Status s = partition->AddSlaveNode(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::RecordNodePartition(const RmNode& slave) {
  std::string index = slave.Ip() + ":" + std::to_string(slave.Port());
  slash::MutexLock l(&node_partitions_mu_);
  std::vector<RmNode>& partitions = node_partitions_[index];
  for (size_t i = 0; i < partitions.size(); ++i) {
    if (slave == partitions[i]) {
      return Status::OK();
    }
  }
  node_partitions_[index].push_back(slave);
  return Status::OK();
}

Status PikaReplicaManager::RemovePartitionSlave(const RmNode& slave) {
  Status s = RemoveSlave(slave);
  if (!s.ok()) {
    return s;
  }
  s = EraseNodePartition(slave);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::RemoveSlave(const RmNode& slave) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  Status s = partition->RemoveSlaveNode(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::EraseNodePartition(const RmNode& slave) {
  std::string index = slave.Ip() + ":" + std::to_string(slave.Port());
  slash::MutexLock l(&node_partitions_mu_);
  if (node_partitions_.find(index) == node_partitions_.end()) {
    return Status::NotFound(slave.ToString());
  }
  std::vector<RmNode>& rm_nodes = node_partitions_[index];
  for (size_t i = 0; i < rm_nodes.size(); ++i) {
    if (rm_nodes[i] == slave) {
      rm_nodes.erase(rm_nodes.begin() + i);
      return Status::OK();
    }
  }
  return Status::NotFound("slave " + slave.ToString());
}

Status PikaReplicaManager::LostConnection(const std::string& ip, int port) {
  std::string index = ip + ":" + std::to_string(port);
  slash::MutexLock l(&node_partitions_mu_);
  if (node_partitions_.find(index) == node_partitions_.end()) {
    return Status::NotFound(index + " not found");
  }
  std::vector<RmNode>& rm_nodes = node_partitions_[index];
  for (auto& rm_node : rm_nodes) {
    Status s = RemoveSlave(rm_node);
    if (!s.ok()) {
      return s;
    }
  }
  node_partitions_.erase(index);
  return Status::OK();
}

Status PikaReplicaManager::ActivateBinlogSync(const RmNode& slave, const BinlogOffset& offset) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> sync_partition = sync_partitions_[slave.NodePartitionInfo()];

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
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  Status s = partition->ActivateSlaveDbSync(slave.Ip(), slave.Port());
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status PikaReplicaManager::SetLastActiveTime(const RmNode& slave, uint64_t time) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  partition->SetLastActiveTime(slave.Ip(), slave.Port(), time);
  return Status::OK();
}

Status PikaReplicaManager::GetLastActiveTime(const RmNode& slave, uint64_t* time) {
  slash::RWLock l(&partitions_rw_, false);
  if (sync_partitions_.find(slave.NodePartitionInfo()) == sync_partitions_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  std::shared_ptr<SyncPartition> partition = sync_partitions_[slave.NodePartitionInfo()];
  partition->GetLastActiveTime(slave.Ip(), slave.Port(), time);
  return Status::OK();
}

Status PikaReplicaManager::WakeUpBinlogSync() {
  slash::RWLock l(&partitions_rw_, false);
  for (auto& iter : sync_partitions_) {
    std::shared_ptr<SyncPartition> partition = iter.second;
    Status s = partition->WakeUpSlaveBinlogSync();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

