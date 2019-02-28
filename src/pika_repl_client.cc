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
}

PikaReplClient::~PikaReplClient() {
  client_thread_->StopThread();
  delete client_thread_;
  for (auto iter : slave_binlog_readers_) {
    delete iter.second;
  }
  LOG(INFO) << "PikaReplClient exit!!!";
}

static inline void BuildBinlogReaderIndex(const RmNode& slave, std::string* index) {
  *index = slave.table_ + "_" + std::to_string(slave.partition_) + "_" + slave.ip_ + ":" + std::to_string(slave.port_);
}

int PikaReplClient::Start() {
  int res = client_thread_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ReplClient ClientThread Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

void PikaReplClient::ProduceWriteQueue(WriteTask& task) {
  slash::MutexLock l(&write_queue_mu_);
  std::string index = task.rm_node_.ip_ + ":" + std::to_string(task.rm_node_.port_);
  write_queues_[index].push(task);
}

void PikaReplClient::ConsumeWriteQueue() {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);

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
}

Status PikaReplClient::Write(const std::string& ip, const int port, const std::string& msg) {
  // shift port 3000 tobe inner connect port
  return client_thread_->Write(ip, port, msg);
}

Status PikaReplClient::RemoveBinlogReader(const RmNode& slave) {
  std::string index;
  BuildBinlogReaderIndex(slave, &index);
  if (slave_binlog_readers_.find(index) != slave_binlog_readers_.end()) {
    delete slave_binlog_readers_[index];
    slave_binlog_readers_.erase(index);
  }
  return Status::OK();
}

Status PikaReplClient::AddBinlogReader(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset) {
  std::string index;
  BuildBinlogReaderIndex(slave, &index);
  RemoveBinlogReader(slave);
  PikaBinlogReader* binlog_reader = NewPikaBinlogReader(logger, filenum, offset);
  if (!binlog_reader) {
    return Status::Corruption(index + " new binlog reader failed");
  }
  int res = binlog_reader->Seek();
  if (res) {
    delete binlog_reader;
    return Status::Corruption(index + "  binlog reader init failed");
  }
  slave_binlog_readers_[index] = binlog_reader;
  return Status::OK();
}

void PikaReplClient::RunStateMachine(const RmNode& slave) {
  Status s = SendSyncBinlog(slave);
  if (!s.ok()) {
    LOG(INFO) << s.ToString();
    return;
  }
  //state = GetState(slave);
  //bool res = false;
  //switch(state) {
  //  case ShouldSendAuth :
  //    res = TrySyncBinlog(slave, false);
  //    break;
  //  case ShouldSendBinlog :
  //    TrySyncBinlog(slave, true);
  //    break;
  //  default:
  //    break;
  //}
}

bool PikaReplClient::NeedToSendBinlog(const RmNode& slave) {
  std::string index;
  BuildBinlogReaderIndex(slave, &index);
  auto binlog_reader_iter = slave_binlog_readers_.find(index);
  if (binlog_reader_iter == slave_binlog_readers_.end()) {
    return false;
  }
  PikaBinlogReader* reader = binlog_reader_iter->second;
  return !(reader->ReadToTheEnd());
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

Status PikaReplClient::SendSyncBinlog(const RmNode& slave) {
  if (!NeedToSendBinlog(slave)) {
    return Status::OK();
  }
  for (int i = 0; i < kBinlogSyncBatchNum; ++i) {
    std::string msg;
    uint32_t filenum;
    uint64_t offset;
    Status s = BuildBinlogMsgFromFile(slave, &msg, &filenum, &offset);
    if (s.IsEndFile()) {
      break;
    } else if (s.IsCorruption() || s.IsIOError()) {
      return s;
    }
    WriteTask task(slave, BinlogChip(filenum, offset, msg));
    ProduceWriteQueue(task);
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

Status PikaReplClient::BuildBinlogMsgFromFile(const RmNode& slave, std::string* scratch, uint32_t* filenum, uint64_t* offset) {
  std::string index;
  BuildBinlogReaderIndex(slave, &index);
  auto iter = slave_binlog_readers_.find(index);
  if (iter == slave_binlog_readers_.end()) {
    return Status::NotFound(index + " not found");
  }
  PikaBinlogReader* reader = iter->second;

  // Get command supports append binlog
  Status s = reader->Get(scratch, filenum, offset);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
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
