// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server.h"

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplServer::PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval) {
  server_tp_ = std::make_unique<net::ThreadPool>(PIKA_REPL_SERVER_TP_SIZE, 100000);
  pika_repl_server_thread_ = std::make_unique<PikaReplServerThread>(ips, port, cron_interval);
  pika_repl_server_thread_->set_thread_name("PikaReplServer");
}

PikaReplServer::~PikaReplServer() {
  LOG(INFO) << "PikaReplServer exit!!!";
}

int PikaReplServer::Start() {
  int res = pika_repl_server_thread_->StartThread();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start Pika Repl Server Thread Error: " << res
               << (res == net::kBindError
                       ? ": bind port " + std::to_string(pika_repl_server_thread_->ListenPort()) + " conflict"
                       : ": create thread error ")
               << ", Listen on this port to handle the request sent by the Slave";
  }
  res = server_tp_->start_thread_pool();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start ThreadPool Error: " << res
               << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

int PikaReplServer::Stop() {
  server_tp_->stop_thread_pool();
  pika_repl_server_thread_->StopThread();
  return 0;
}

pstd::Status PikaReplServer::SendSlaveBinlogChips(const std::string& ip, int port,
                                                  const std::vector<WriteTask>& tasks) {
  InnerMessage::InnerResponse response;
  BuildBinlogSyncResp(tasks, &response);

  std::string binlog_chip_pb;
  if (!response.SerializeToString(&binlog_chip_pb)) {
    return Status::Corruption("Serialized Failed");
  }

  if (binlog_chip_pb.size() > static_cast<size_t>(g_pika_conf->max_conn_rbuf_size())) {
    for (const auto& task : tasks) {
      InnerMessage::InnerResponse response;
      std::vector<WriteTask> tmp_tasks;
      tmp_tasks.push_back(task);
      BuildBinlogSyncResp(tmp_tasks, &response);
      if (!response.SerializeToString(&binlog_chip_pb)) {
        return Status::Corruption("Serialized Failed");
      }
      pstd::Status s = Write(ip, port, binlog_chip_pb);
      if (!s.ok()) {
        return s;
      }
    }
    return pstd::Status::OK();
  }
  return Write(ip, port, binlog_chip_pb);
}

void PikaReplServer::BuildBinlogOffset(const LogOffset& offset, InnerMessage::BinlogOffset* boffset) {
  boffset->set_filenum(offset.b_offset.filenum);
  boffset->set_offset(offset.b_offset.offset);
  boffset->set_term(offset.l_offset.term);
  boffset->set_index(offset.l_offset.index);
}

void PikaReplServer::BuildBinlogSyncResp(const std::vector<WriteTask>& tasks, InnerMessage::InnerResponse* response) {
  response->set_code(InnerMessage::kOk);
  response->set_type(InnerMessage::Type::kBinlogSync);
  LogOffset prev_offset;
  bool founded = false;
  for (const auto& task : tasks) {
    if (!task.binlog_chip_.binlog_.empty() && !founded) {
      // find the first not keepalive prev_offset
      prev_offset = task.prev_offset_;
      founded = true;
    }
    InnerMessage::InnerResponse::BinlogSync* binlog_sync = response->add_binlog_sync();
    binlog_sync->set_session_id(task.rm_node_.SessionId());
    InnerMessage::Partition* partition = binlog_sync->mutable_partition();
    partition->set_table_name(task.rm_node_.TableName());
    partition->set_partition_id(task.rm_node_.PartitionId());
    InnerMessage::BinlogOffset* boffset = binlog_sync->mutable_binlog_offset();
    BuildBinlogOffset(task.binlog_chip_.offset_, boffset);
    binlog_sync->set_binlog(task.binlog_chip_.binlog_);
  }
  if (g_pika_conf->consensus_level() > 0) {
    PartitionInfo p_info;
    if (!tasks.empty()) {
      p_info = tasks[0].rm_node_.NodePartitionInfo();
    } else {
      LOG(WARNING) << "Task size is zero";
      return;
    }

    // write consensus_meta
    InnerMessage::ConsensusMeta* consensus_meta = response->mutable_consensus_meta();
    InnerMessage::BinlogOffset* last_log = consensus_meta->mutable_log_offset();
    BuildBinlogOffset(prev_offset, last_log);
    // commit
    LogOffset committed_index;
    std::shared_ptr<SyncMasterPartition> partition = g_pika_rm->GetSyncMasterPartitionByName(p_info);
    if (!partition) {
      LOG(WARNING) << "SyncPartition " << p_info.ToString() << " Not Found.";
      return;
    }
    committed_index = partition->ConsensusCommittedIndex();
    InnerMessage::BinlogOffset* committed = consensus_meta->mutable_commit();
    BuildBinlogOffset(committed_index, committed);
    consensus_meta->set_term(partition->ConsensusTerm());
  }
}

pstd::Status PikaReplServer::Write(const std::string& ip, const int port, const std::string& msg) {
  std::shared_lock l(client_conn_rwlock_);
  const std::string ip_port = pstd::IpPortString(ip, port);
  if (client_conn_map_.find(ip_port) == client_conn_map_.end()) {
    return Status::NotFound("The " + ip_port + " fd cannot be found");
  }
  int fd = client_conn_map_[ip_port];
  std::shared_ptr<net::PbConn> conn = std::dynamic_pointer_cast<net::PbConn>(pika_repl_server_thread_->get_conn(fd));
  if (!conn) {
    return Status::NotFound("The" + ip_port + " conn cannot be found");
  }

  if (conn->WriteResp(msg)) {
    conn->NotifyClose();
    return Status::Corruption("The" + ip_port + " conn, Write Resp Failed");
  }
  conn->NotifyWrite();
  return Status::OK();
}

void PikaReplServer::Schedule(net::TaskFunc func, void* arg) { server_tp_->Schedule(func, arg); }

void PikaReplServer::UpdateClientConnMap(const std::string& ip_port, int fd) {
  std::lock_guard l(client_conn_rwlock_);
  client_conn_map_[ip_port] = fd;
}

void PikaReplServer::RemoveClientConn(int fd) {
  std::lock_guard l(client_conn_rwlock_);
  auto iter = client_conn_map_.begin();
  while (iter != client_conn_map_.end()) {
    if (iter->second == fd) {
      iter = client_conn_map_.erase(iter);
      break;
    }
    iter++;
  }
}

void PikaReplServer::KillAllConns() { return pika_repl_server_thread_->KillAllConns(); }
