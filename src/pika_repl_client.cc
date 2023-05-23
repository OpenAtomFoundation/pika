// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <utility>

#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"
#include "pstd/include/env.h"
#include "pstd/include/pstd_coding.h"
#include "pstd/include/pstd_string.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplClient::PikaReplClient(int cron_interval, int keepalive_timeout)  {
  client_thread_ = std::make_unique<PikaReplClientThread>(cron_interval, keepalive_timeout);
  client_thread_->set_thread_name("PikaReplClient");
  for (int i = 0; i < 2 * g_pika_conf->sync_thread_num(); ++i) {
    bg_workers_.push_back(std::make_unique<PikaReplBgWorker>(PIKA_SYNC_BUFFER_SIZE));
  }
}

PikaReplClient::~PikaReplClient() {
  client_thread_->StopThread();
  LOG(INFO) << "PikaReplClient exit!!!";
}

int PikaReplClient::Start() {
  int res = client_thread_->StartThread();
  if (res != net::kSuccess) {
    LOG(FATAL) << "Start ReplClient ClientThread Error: " << res
               << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
  }
  for (auto & bg_worker : bg_workers_) {
    res = bg_worker->StartThread();
    if (res != net::kSuccess) {
      LOG(FATAL) << "Start Pika Repl Worker Thread Error: " << res
                 << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
    }
  }
  return res;
}

int PikaReplClient::Stop() {
  client_thread_->StopThread();
  for (auto & bg_worker : bg_workers_) {
    bg_worker->StopThread();
  }
  return 0;
}

void PikaReplClient::Schedule(net::TaskFunc func, void* arg) {
  bg_workers_[next_avail_]->Schedule(func, arg);
  UpdateNextAvail();
}

void PikaReplClient::ScheduleWriteBinlogTask(const std::string& table_partition,
                                             const std::shared_ptr<InnerMessage::InnerResponse>& res,
                                             std::shared_ptr<net::PbConn> conn, void* res_private_data) {
  size_t index = GetHashIndex(table_partition, true);
  auto task_arg = new ReplClientWriteBinlogTaskArg(res, std::move(conn), res_private_data, bg_workers_[index].get());
  bg_workers_[index]->Schedule(&PikaReplBgWorker::HandleBGWorkerWriteBinlog, static_cast<void*>(task_arg));
}

void PikaReplClient::ScheduleWriteDBTask(const std::shared_ptr<Cmd>& cmd_ptr, const LogOffset& offset,
                                         const std::string& table_name, uint32_t partition_id) {
  const PikaCmdArgsType& argv = cmd_ptr->argv();
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  size_t index = GetHashIndex(dispatch_key, false);
  auto* task_arg = new ReplClientWriteDBTaskArg(cmd_ptr, offset, table_name, partition_id);
  bg_workers_[index]->Schedule(&PikaReplBgWorker::HandleBGWorkerWriteDB, static_cast<void*>(task_arg));
}

size_t PikaReplClient::GetHashIndex(const std::string& key, bool upper_half) {
  size_t hash_base = bg_workers_.size() / 2;
  return (str_hash(key) % hash_base) + (upper_half ? 0 : hash_base);
}

Status PikaReplClient::Write(const std::string& ip, const int port, const std::string& msg) {
  return client_thread_->Write(ip, port, msg);
}

Status PikaReplClient::Close(const std::string& ip, const int port) { return client_thread_->Close(ip, port); }

Status PikaReplClient::SendMetaSync() {
  std::string local_ip;
  std::unique_ptr<net::NetCli> cli (net::NewRedisCli());
  cli->set_connect_timeout(1500);
  if ((cli->Connect(g_pika_server->master_ip(), g_pika_server->master_port(), "")).ok()) {
    struct sockaddr_in laddr;
    socklen_t llen = sizeof(laddr);
    getsockname(cli->fd(), reinterpret_cast<struct sockaddr*>(&laddr), &llen);
    std::string tmp_local_ip(inet_ntoa(laddr.sin_addr));
    local_ip = tmp_local_ip;
    cli->Close();
  } else {
    LOG(WARNING) << "Failed to connect master, Master (" << g_pika_server->master_ip() << ":"
                 << g_pika_server->master_port() << "), try reconnect";
    // Sleep three seconds to avoid frequent try Meta Sync
    // when the connection fails
    sleep(3);
    g_pika_server->ResetMetaSyncStatus();
    return Status::Corruption("Connect master error");
  }

  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kMetaSync);
  InnerMessage::InnerRequest::MetaSync* meta_sync = request.mutable_meta_sync();
  InnerMessage::Node* node = meta_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());

  std::string masterauth = g_pika_conf->masterauth();
  if (!masterauth.empty()) {
    meta_sync->set_auth(masterauth);
  }

  std::string to_send;
  std::string master_ip = g_pika_server->master_ip();
  int master_port = g_pika_server->master_port();
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Meta Sync Request Failed, to Master (" << master_ip << ":" << master_port << ")";
    return Status::Corruption("Serialize Failed");
  }

  LOG(INFO) << "Try Send Meta Sync Request to Master (" << master_ip << ":" << master_port << ")";
  return client_thread_->Write(master_ip, master_port + kPortShiftReplServer, to_send);
}

Status PikaReplClient::SendPartitionDBSync(const std::string& ip, uint32_t port, const std::string& table_name,
                                           uint32_t partition_id, const BinlogOffset& boffset,
                                           const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kDBSync);
  InnerMessage::InnerRequest::DBSync* db_sync = request.mutable_db_sync();
  InnerMessage::Node* node = db_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  InnerMessage::Partition* partition = db_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = db_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition DBSync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, port + kPortShiftReplServer, to_send);
}

Status PikaReplClient::SendPartitionTrySync(const std::string& ip, uint32_t port, const std::string& table_name,
                                            uint32_t partition_id, const BinlogOffset& boffset,
                                            const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  InnerMessage::Partition* partition = try_sync->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  InnerMessage::BinlogOffset* binlog_offset = try_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  if (g_pika_conf->consensus_level() != 0) {
    std::shared_ptr<SyncMasterPartition> partition =
        g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
    if (!partition) {
      return Status::Corruption("partition not found");
    }
    LogOffset last_index = partition->ConsensusLastIndex();
    uint32_t term = partition->ConsensusTerm();
    LOG(INFO) << PartitionInfo(table_name, partition_id).ToString() << " TrySync Increase self term from " << term
              << " to " << term + 1;
    term++;
    partition->ConsensusUpdateTerm(term);
    InnerMessage::ConsensusMeta* consensus_meta = request.mutable_consensus_meta();
    consensus_meta->set_term(term);
    InnerMessage::BinlogOffset* pb_offset = consensus_meta->mutable_log_offset();
    pb_offset->set_filenum(last_index.b_offset.filenum);
    pb_offset->set_offset(last_index.b_offset.offset);
    pb_offset->set_term(last_index.l_offset.term);
    pb_offset->set_index(last_index.l_offset.index);
  }

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition TrySync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, port + kPortShiftReplServer, to_send);
}

Status PikaReplClient::SendPartitionBinlogSync(const std::string& ip, uint32_t port, const std::string& table_name,
                                               uint32_t partition_id, const LogOffset& ack_start,
                                               const LogOffset& ack_end, const std::string& local_ip,
                                               bool is_first_send) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);
  InnerMessage::InnerRequest::BinlogSync* binlog_sync = request.mutable_binlog_sync();
  InnerMessage::Node* node = binlog_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  binlog_sync->set_table_name(table_name);
  binlog_sync->set_partition_id(partition_id);
  binlog_sync->set_first_send(is_first_send);

  InnerMessage::BinlogOffset* ack_range_start = binlog_sync->mutable_ack_range_start();
  ack_range_start->set_filenum(ack_start.b_offset.filenum);
  ack_range_start->set_offset(ack_start.b_offset.offset);
  ack_range_start->set_term(ack_start.l_offset.term);
  ack_range_start->set_index(ack_start.l_offset.index);

  InnerMessage::BinlogOffset* ack_range_end = binlog_sync->mutable_ack_range_end();
  ack_range_end->set_filenum(ack_end.b_offset.filenum);
  ack_range_end->set_offset(ack_end.b_offset.offset);
  ack_range_end->set_term(ack_end.l_offset.term);
  ack_range_end->set_index(ack_end.l_offset.index);

  std::shared_ptr<SyncSlavePartition> slave_partition =
      g_pika_rm->GetSyncSlavePartitionByName(PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table_name << "_" << partition_id << " not exist";
    return Status::NotFound("SyncSlavePartition NotFound");
  }
  int32_t session_id = slave_partition->MasterSessionId();
  binlog_sync->set_session_id(session_id);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Partition BinlogSync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, port + kPortShiftReplServer, to_send);
}

Status PikaReplClient::SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& table_name,
                                           uint32_t partition_id, const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kRemoveSlaveNode);
  InnerMessage::InnerRequest::RemoveSlaveNode* remove_slave_node = request.add_remove_slave_node();
  InnerMessage::Node* node = remove_slave_node->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());

  InnerMessage::Partition* partition = remove_slave_node->mutable_partition();
  partition->set_table_name(table_name);
  partition->set_partition_id(partition_id);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Remove Slave Node Failed, to Master (" << ip << ":" << port << "), " << table_name << "_"
                 << partition_id;
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, port + kPortShiftReplServer, to_send);
}
