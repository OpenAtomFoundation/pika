// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_conf.h"
#include "include/pika_server.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaReplServer::PikaReplServer(const std::set<std::string>& ips,
                               int port,
                               int cron_interval) {
  server_tp_ = new pink::ThreadPool(PIKA_REPL_SERVER_TP_SIZE, 100000);
  pika_repl_server_thread_ = new PikaReplServerThread(ips, port, cron_interval);
  pika_repl_server_thread_->set_thread_name("PikaReplServer");
  pthread_rwlock_init(&client_conn_rwlock_, NULL);
}

PikaReplServer::~PikaReplServer() {
  delete pika_repl_server_thread_;
  delete server_tp_;
  pthread_rwlock_destroy(&client_conn_rwlock_);
  LOG(INFO) << "PikaReplServer exit!!!";
}

int PikaReplServer::Start() {
  int res = pika_repl_server_thread_->StartThread();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start Pika Repl Server Thread Error: " << res
        << (res == pink::kBindError ? ": bind port " + std::to_string(pika_repl_server_thread_->ListenPort()) + " conflict" : ": create thread error ")
        << ", Listen on this port to handle the request sent by the Slave";
  }
  res = server_tp_->start_thread_pool();
  if (res != pink::kSuccess) {
    LOG(FATAL) << "Start ThreadPool Error: " << res << (res == pink::kCreateThreadError ? ": create thread error " : ": other error");
  }
  return res;
}

int PikaReplServer::Stop() {
  server_tp_->stop_thread_pool();
  pika_repl_server_thread_->StopThread();
  return 0;
}

slash::Status PikaReplServer::SendSlaveBinlogChips(const std::string& ip,
                                                   int port,
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
      slash::Status s = Write(ip, port, binlog_chip_pb);
      if (!s.ok()) {
        return s;
      }
    }
    return slash::Status::OK();
  }

  return Write(ip, port, binlog_chip_pb);
}

void PikaReplServer::BuildBinlogSyncResp(const std::vector<WriteTask>& tasks,
    InnerMessage::InnerResponse* response) {
  response->set_code(InnerMessage::kOk);
  response->set_type(InnerMessage::Type::kBinlogSync);
  for (const auto& task :tasks) {
    InnerMessage::InnerResponse::BinlogSync* binlog_sync = response->add_binlog_sync();
    binlog_sync->set_session_id(task.rm_node_.SessionId());
    InnerMessage::Partition* partition = binlog_sync->mutable_partition();
    partition->set_table_name(task.rm_node_.TableName());
    partition->set_partition_id(task.rm_node_.PartitionId());
    InnerMessage::BinlogOffset* boffset = binlog_sync->mutable_binlog_offset();
    boffset->set_filenum(task.binlog_chip_.offset_.b_offset.filenum);
    boffset->set_offset(task.binlog_chip_.offset_.b_offset.offset);
    boffset->set_term(task.binlog_chip_.offset_.l_offset.term);
    boffset->set_index(task.binlog_chip_.offset_.l_offset.index);
    binlog_sync->set_binlog(task.binlog_chip_.binlog_);
  }
  if (g_pika_conf->consensus_level() > 0) {
    LogOffset prev_offset;
    PartitionInfo p_info;
    if (!tasks.empty()) {
      prev_offset = tasks[0].prev_offset_;
      p_info = tasks[0].rm_node_.NodePartitionInfo();
    } else {
      return;
    }
    // write consensus_meta
    InnerMessage::ConsensusMeta* consensus_meta
      = response->mutable_consensus_meta();
    InnerMessage::BinlogOffset* last_log = consensus_meta->mutable_log_offset();
    last_log->set_filenum(prev_offset.b_offset.filenum);
    last_log->set_offset(prev_offset.b_offset.offset);
    last_log->set_term(prev_offset.l_offset.term);
    last_log->set_index(prev_offset.l_offset.index);
    // commit
    LogOffset committed_index;
    std::shared_ptr<SyncMasterPartition> partition =
      g_pika_rm->GetSyncMasterPartitionByName(p_info);
    if (!partition) {
      committed_index = partition->ConsensusCommittedIndex();
    } else {
      LOG(WARNING) << "SyncPartition " << p_info.ToString() << " Not Found.";
      return;
    }
    InnerMessage::BinlogOffset* committed = consensus_meta->mutable_commit();
    committed->set_filenum(committed_index.b_offset.filenum);
    committed->set_offset(committed_index.b_offset.offset);
    committed->set_term(committed_index.l_offset.term);
    committed->set_index(committed_index.l_offset.index);
  }
}

slash::Status PikaReplServer::Write(const std::string& ip,
                                    const int port,
                                    const std::string& msg) {
  slash::RWLock l(&client_conn_rwlock_, false);
  const std::string ip_port = slash::IpPortString(ip, port);
  if (client_conn_map_.find(ip_port) == client_conn_map_.end()) {
    return Status::NotFound("The " + ip_port + " fd cannot be found");
  }
  int fd = client_conn_map_[ip_port];
  std::shared_ptr<pink::PbConn> conn =
      std::dynamic_pointer_cast<pink::PbConn>(pika_repl_server_thread_->get_conn(fd));
  if (conn == nullptr) {
    return Status::NotFound("The" + ip_port + " conn cannot be found");
  }

  if (conn->WriteResp(msg)) {
    conn->NotifyClose();
    return Status::Corruption("The" + ip_port + " conn, Write Resp Failed");
  }
  conn->NotifyWrite();
  return Status::OK();
}

void PikaReplServer::Schedule(pink::TaskFunc func, void* arg){
  server_tp_->Schedule(func, arg);
}

void PikaReplServer::UpdateClientConnMap(const std::string& ip_port, int fd) {
  slash::RWLock l(&client_conn_rwlock_, true);
  client_conn_map_[ip_port] = fd;
}

void PikaReplServer::RemoveClientConn(int fd) {
  slash::RWLock l(&client_conn_rwlock_, true);
  std::map<std::string, int>::const_iterator iter = client_conn_map_.begin();
  while (iter != client_conn_map_.end()) {
    if (iter->second == fd) {
      iter = client_conn_map_.erase(iter);
      break;
    }
    iter++;
  }
}

void PikaReplServer::KillAllConns() {
  return pika_repl_server_thread_->KillAllConns();
}

