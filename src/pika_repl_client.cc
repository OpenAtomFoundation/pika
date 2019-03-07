// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client.h"

#include "include/pika_server.h"
#include "slash/include/slash_coding.h"
#include "slash/include/env.h"
#include "slash/include/slash_string.h"

extern PikaServer* g_pika_server;

PikaReplClient::PikaReplClient(int cron_interval, int keepalive_timeout) {
  client_thread_ = new PikaReplClientThread(cron_interval, keepalive_timeout);
  client_thread_->set_thread_name("PikaReplClient");
  client_tp_ = new pink::ThreadPool(g_pika_conf->thread_pool_size(), 100000);
  pthread_rwlock_init(&binlog_ctl_rw_, NULL);
}

PikaReplClient::~PikaReplClient() {
  client_thread_->StopThread();
  delete client_thread_;
  for (auto iter : binlog_ctl_) {
    delete iter.second;
  }
  delete client_tp_;
  pthread_rwlock_destroy(&binlog_ctl_rw_);
  LOG(INFO) << "PikaReplClient exit!!!";
}

int PikaReplClient::Start() {
  int res = client_thread_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ReplClient ClientThread Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  res = client_tp_->start_thread_pool();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ThreadPool Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

void PikaReplClient::ProduceWriteQueue(WriteTask& task) {
  slash::MutexLock l(&write_queue_mu_);
  std::string index = task.rm_node_.ip_ + ":" + std::to_string(task.rm_node_.port_);
  write_queues_[index].push(task);
}

int PikaReplClient::ConsumeWriteQueue() {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);

  int counter = 0;
  slash::MutexLock l(&write_queue_mu_);
  for (auto& iter : write_queues_) {
    std::queue<WriteTask>& queue = iter.second;
    if (queue.empty()) {
      continue;
    }
    size_t batch_index = queue.size() > kBinlogSyncBatchNum ? kBinlogSyncBatchNum : queue.size();
    std::string ip;
    int port = 0;
    if (!slash::ParseIpPortString(iter.first, ip, port)) {
      LOG(WARNING) << "Parse ip_port error " << iter.first;
    }
    for (size_t i = 0; i < batch_index; ++i) {
      WriteTask task = queue.front();
      queue.pop();
      BuildBinlogPb(task.rm_node_,
                    task.binlog_chip_.binlog_,
                    task.binlog_chip_.file_num_,
                    task.binlog_chip_.offset_, request);

      counter++;
    }
    std::string to_send;
    bool res = request.SerializeToString(&to_send);
    if (!res) {
      LOG(WARNING) << "Serialize Failed";
      continue;
    }
    Status s = client_thread_->Write(ip, port, to_send);
    if (!s.ok()) {
      LOG(WARNING) << "write to " << ip << ":" << port << " failed";
      continue;
    }
  }
  return counter;
}

bool PikaReplClient::SetAckInfo(const RmNode& slave, uint32_t ack_file_num, uint64_t ack_offset, uint64_t active_time) {
  BinlogSyncCtl* ctl = nullptr;
  {
  slash::RWLock l(&binlog_ctl_rw_, false);
  if (binlog_ctl_.find(slave) == binlog_ctl_.end()) {
    return false;
  }
  ctl = binlog_ctl_[slave];
  }

  {
  slash::MutexLock l(&(ctl->ctl_mu_));
  ctl->ack_file_num_ = ack_file_num;
  ctl->ack_offset_ = ack_offset;
  ctl->active_time_ = active_time;
  }
  return true;
}

bool PikaReplClient::GetAckInfo(const RmNode& slave, uint32_t* ack_file_num, uint64_t* ack_offset, uint64_t* active_time) {
  BinlogSyncCtl* ctl = nullptr;
  {
  slash::RWLock l(&binlog_ctl_rw_, false);
  if (binlog_ctl_.find(slave) == binlog_ctl_.end()) {
    return false;
  }
  ctl = binlog_ctl_[slave];
  }

  {
  slash::MutexLock l(&(ctl->ctl_mu_));
  *ack_file_num = ctl->ack_file_num_;
  *ack_offset = ctl->ack_offset_;
  *active_time = ctl->active_time_;
  }
  return true;
}

Status PikaReplClient::Write(const std::string& ip, const int port, const std::string& msg) {
  // shift port 3000 tobe inner connect port
  return client_thread_->Write(ip, port, msg);
}

Status PikaReplClient::RemoveBinlogReader(const RmNode& slave) {
  {
  slash::RWLock l(&binlog_ctl_rw_, true);
  if (binlog_ctl_.find(slave) != binlog_ctl_.end()) {
    delete binlog_ctl_[slave];
    binlog_ctl_.erase(slave);
  }
  }
  return Status::OK();
}

Status PikaReplClient::AddBinlogReader(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset) {
  RemoveBinlogReader(slave);
  PikaBinlogReader* binlog_reader = NewPikaBinlogReader(logger, filenum, offset);
  if (!binlog_reader) {
    return Status::Corruption(slave.ToString() + " new binlog reader failed");
  }
  int res = binlog_reader->Seek();
  if (res) {
    delete binlog_reader;
    return Status::Corruption(slave.ToString() + "  binlog reader init failed");
  }
  uint32_t cur_file_num;
  uint64_t cur_offset;
  binlog_reader->GetReaderStatus(&cur_file_num, &cur_offset);

  {
  slash::RWLock l(&binlog_ctl_rw_, true);
  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  binlog_ctl_[slave] = new BinlogSyncCtl(binlog_reader, cur_file_num, cur_offset, now);
  }
  return Status::OK();
}

Status PikaReplClient::SendMetaSync() {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kMetaSync);
  InnerMessage::InnerRequest::MetaSync* meta_sync = request.mutable_meta_sync();
  InnerMessage::Node* node = meta_sync->mutable_node();
  node->set_ip(g_pika_server->host());
  node->set_port(g_pika_server->port());

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    return Status::Corruption("Serialize Failed");
  }

  std::string master_ip = g_pika_server->master_ip();
  int master_port = g_pika_server->master_port();
  LOG(INFO) << "Send Meta Sync Request to Master ("
    << master_ip << ":" << master_port << ")";
  return client_thread_->Write(master_ip, master_port + 3000, to_send);
}

Status PikaReplClient::SendPartitionTrySync(const std::string& table_name,
                                            uint32_t partition_id,
                                            const BinlogOffset& boffset) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(g_pika_server->host());
  node->set_port(g_pika_server->port());
  InnerMessage::Partition* partition = try_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  bool force_sync = g_pika_server->force_full_sync();
  try_sync->set_force(force_sync);
  InnerMessage::BinlogOffset* binlog_offset = try_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(force_sync ? 0 : boffset.filenum);
  binlog_offset->set_offset(force_sync ? 0 : boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    return Status::Corruption("Serialize Failed");
  }

  std::string master_ip = g_pika_server->master_ip();
  int master_port = g_pika_server->master_port();
  return client_thread_->Write(master_ip, master_port + 3000, to_send);
}


Status PikaReplClient::SendBinlogSync(const RmNode& slave) {
  BinlogSyncCtl* ctl = nullptr;
  {
  slash::RWLock l(&binlog_ctl_rw_, false);
  auto iter = binlog_ctl_.find(slave);
  if (iter == binlog_ctl_.end()) {
    return Status::NotFound(slave.ToString() + " not found");
  }
  ctl = iter->second;
  }

  {
  slash::MutexLock l(&(ctl->ctl_mu_));
  for (int i = 0; i < kBinlogSyncBatchNum; ++i) {
    std::string msg;
    uint32_t filenum;
    uint64_t offset;
    Status s = ctl->reader_->Get(&msg, &filenum, &offset);
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      return s;
    }
    WriteTask task(slave, BinlogChip(filenum, offset, msg));
    ProduceWriteQueue(task);
  }
  }
  return Status::OK();
}

Status PikaReplClient::TriggerSendBinlogSync() {
  slash::RWLock l(&binlog_ctl_rw_, false);
  for (auto& binlog_ctl : binlog_ctl_) {
    BinlogSyncCtl* ctl = binlog_ctl.second;
    {
    slash::MutexLock l(&(ctl->ctl_mu_));
    uint32_t send_file_num;
    uint64_t send_offset;
    ctl->reader_->GetReaderStatus(&send_file_num, &send_offset);
    if (ctl->ack_file_num_ == send_file_num && ctl->ack_offset_ == send_offset) {
      for (int i = 0; i < kBinlogSyncBatchNum; ++i) {
        std::string msg;
        uint32_t filenum;
        uint64_t offset;
        Status s = ctl->reader_->Get(&msg, &filenum, &offset);
        if (s.IsEndFile()) {
          break;
        } else if (s.IsCorruption() || s.IsIOError()) {
          return s;
        }
        WriteTask task(binlog_ctl.first, BinlogChip(filenum, offset, msg));
        ProduceWriteQueue(task);
      }
    }
    }
  }
  return Status::OK();
}

void PikaReplClient::BuildBinlogPb(const RmNode& slave, const std::string& msg, uint32_t filenum, uint64_t offset, InnerMessage::InnerRequest& request) {
  InnerMessage::InnerRequest::BinlogSync* binlog_msg = request.add_binlog_sync();
  InnerMessage::Node* node = binlog_msg->mutable_node();
  node->set_ip(slave.ip_);
  node->set_port(slave.port_);
  binlog_msg->set_table_name(slave.table_);
  binlog_msg->set_partition_id(slave.partition_);
  InnerMessage::BinlogOffset* binlog_offset = binlog_msg->mutable_binlog_offset();
  binlog_offset->set_filenum(filenum);
  binlog_offset->set_offset(offset);
  binlog_msg->set_binlog(msg);
}

PikaBinlogReader* PikaReplClient::NewPikaBinlogReader(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset) {
  std::string confile = NewFileName(logger->filename, filenum);
  if (!slash::FileExists(confile)) {
    return NULL;
  }
  slash::SequentialFile* readfile;
  if (!slash::NewSequentialFile(confile, &readfile).ok()) {
    return NULL;
  }
  return new PikaBinlogReader(readfile, logger, filenum, offset);
}
