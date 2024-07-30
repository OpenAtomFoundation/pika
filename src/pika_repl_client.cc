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

PikaReplClient::PikaReplClient(int cron_interval, int keepalive_timeout) {
  for (int i = 0; i < MAX_DB_NUM; i++) {
    async_write_db_task_counts_[i].store(0, std::memory_order::memory_order_seq_cst);
  }
  client_thread_ = std::make_unique<PikaReplClientThread>(cron_interval, keepalive_timeout);
  client_thread_->set_thread_name("PikaReplClient");
  for (int i = 0; i < g_pika_conf->sync_binlog_thread_num(); i++) {
      auto new_binlog_worker = std::make_unique<PikaReplBgWorker>(PIKA_SYNC_BUFFER_SIZE);
      std::string binlog_worker_name = "ReplBinlogWorker" + std::to_string(i);
      new_binlog_worker->SetThreadName(binlog_worker_name);
      write_binlog_workers_.emplace_back(std::move(new_binlog_worker));
  }
  for (int i = 0; i < g_pika_conf->sync_thread_num(); ++i) {
      auto new_db_worker = std::make_unique<PikaReplBgWorker>(PIKA_SYNC_BUFFER_SIZE);
      std::string db_worker_name = "ReplWriteDBWorker" + std::to_string(i);
      new_db_worker->SetThreadName(db_worker_name);
      write_db_workers_.emplace_back(std::move(new_db_worker));
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
  for (auto & binlog_worker : write_binlog_workers_) {
    res = binlog_worker->StartThread();
    if (res != net::kSuccess) {
      LOG(FATAL) << "Start Pika Repl Write Binlog Worker Thread Error: " << res
                 << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
    }
  }
  for (auto & db_worker : write_db_workers_) {
        res = db_worker->StartThread();
        if (res != net::kSuccess) {
            LOG(FATAL) << "Start Pika Repl Write DB Worker Thread Error: " << res
                       << (res == net::kCreateThreadError ? ": create thread error " : ": other error");
        }
    }
  return res;
}

int PikaReplClient::Stop() {
  client_thread_->StopThread();
  for (auto & binlog_worker : write_binlog_workers_) {
    binlog_worker->StopThread();
  }
  for (auto &db_worker: write_db_workers_) {
    db_worker->StopThread();
  }
  return 0;
}

void PikaReplClient::Schedule(net::TaskFunc func, void* arg) {
  write_binlog_workers_[next_avail_]->Schedule(func, arg);
  UpdateNextAvail();
}

void PikaReplClient::ScheduleByDBName(net::TaskFunc func, void* arg, const std::string& db_name) {
  size_t index = GetBinlogWorkerIndexByDBName(db_name);
  write_binlog_workers_[index]->Schedule(func, arg);
};

void PikaReplClient::ScheduleWriteBinlogTask(const std::string& db_name,
                                             const std::shared_ptr<InnerMessage::InnerResponse>& res,
                                             const std::shared_ptr<net::PbConn>& conn, void* res_private_data) {
  size_t index = GetBinlogWorkerIndexByDBName(db_name);
  auto task_arg = new ReplClientWriteBinlogTaskArg(res, conn, res_private_data, write_binlog_workers_[index].get());
  write_binlog_workers_[index]->Schedule(&PikaReplBgWorker::HandleBGWorkerWriteBinlog, static_cast<void*>(task_arg));
}

void PikaReplClient::ScheduleWriteDBTask(std::shared_ptr<Cmd> cmd_ptr, const std::string& db_name) {
  const PikaCmdArgsType& argv = cmd_ptr->argv();
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  size_t index = GetHashIndexByKey(dispatch_key);
  auto task_arg = new ReplClientWriteDBTaskArg(std::move(cmd_ptr));

  IncrAsyncWriteDBTaskCount(db_name, 1);
  std::function<void()> task_finish_call_back = [this, db_name]() { this->DecrAsyncWriteDBTaskCount(db_name, 1); };

  write_db_workers_[index]->Schedule(&PikaReplBgWorker::HandleBGWorkerWriteDB, static_cast<void*>(task_arg),
                                     task_finish_call_back);
}

size_t PikaReplClient::GetBinlogWorkerIndexByDBName(const std::string &db_name) {
    char db_num_c = db_name.back();
    int32_t db_num = db_num_c - '0';
    //Valid range of db_num is [0, MAX_DB_NUM)
    if (db_num < 0 || db_num >= MAX_DB_NUM) {
        LOG(ERROR)
                << "Corruption in consuming binlog: the last char of the db_name(extracted from binlog) is not a valid db num, the extracted db_num is "
                << db_num_c << " while write_binlog_workers.size() is " << write_binlog_workers_.size();
        if (db_num < 0) { assert(false && "db_num invalid, check if the db_name in the request is valid, also check the ERROR Log of Pika."); }
    }
    return db_num % write_binlog_workers_.size();
}

size_t PikaReplClient::GetHashIndexByKey(const std::string& key) {
  size_t hash_base = write_db_workers_.size();
  return (str_hash(key) % hash_base);
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

Status PikaReplClient::SendDBSync(const std::string& ip, uint32_t port, const std::string& db_name,
                                  const BinlogOffset& boffset, const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kDBSync);
  InnerMessage::InnerRequest::DBSync* db_sync = request.mutable_db_sync();
  InnerMessage::Node* node = db_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  InnerMessage::Slot* db = db_sync->mutable_slot();
  db->set_db_name(db_name);
  /*
   * Since the slot field is written in protobuffer,
   * slot_id is set to the default value 0 for compatibility
   * with older versions, but slot_id is not used
   */
  db->set_slot_id(0);

  InnerMessage::BinlogOffset* binlog_offset = db_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize DB DBSync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, static_cast<int32_t>(port) + kPortShiftReplServer, to_send);
}

Status PikaReplClient::SendTrySync(const std::string& ip, uint32_t port, const std::string& db_name,
                                   const BinlogOffset& boffset, const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kTrySync);
  InnerMessage::InnerRequest::TrySync* try_sync = request.mutable_try_sync();
  InnerMessage::Node* node = try_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  InnerMessage::Slot* db = try_sync->mutable_slot();
  db->set_db_name(db_name);
  /*
   * Since the slot field is written in protobuffer,
   * slot_id is set to the default value 0 for compatibility
   * with older versions, but slot_id is not used
   */
  db->set_slot_id(0);

  InnerMessage::BinlogOffset* binlog_offset = try_sync->mutable_binlog_offset();
  binlog_offset->set_filenum(boffset.filenum);
  binlog_offset->set_offset(boffset.offset);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize DB TrySync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, static_cast<int32_t>(port + kPortShiftReplServer), to_send);
}

Status PikaReplClient::SendBinlogSync(const std::string& ip, uint32_t port, const std::string& db_name,
                                      const LogOffset& ack_start, const LogOffset& ack_end,
                                      const std::string& local_ip, bool is_first_send) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kBinlogSync);
  InnerMessage::InnerRequest::BinlogSync* binlog_sync = request.mutable_binlog_sync();
  InnerMessage::Node* node = binlog_sync->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());
  binlog_sync->set_db_name(db_name);
  /*
   * Since the slot field is written in protobuffer,
   * slot_id is set to the default value 0 for compatibility
   * with older versions, but slot_id is not used
   */
  binlog_sync->set_slot_id(0);
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

  std::shared_ptr<SyncSlaveDB> slave_db =
      g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << " not exist";
    return Status::NotFound("SyncSlaveDB NotFound");
  }
  int32_t session_id = slave_db->MasterSessionId();
  binlog_sync->set_session_id(session_id);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize DB BinlogSync Request Failed, to Master (" << ip << ":" << port << ")";
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, static_cast<int32_t>(port + kPortShiftReplServer), to_send);
}

Status PikaReplClient::SendRemoveSlaveNode(const std::string& ip, uint32_t port, const std::string& db_name,
                                           const std::string& local_ip) {
  InnerMessage::InnerRequest request;
  request.set_type(InnerMessage::kRemoveSlaveNode);
  InnerMessage::InnerRequest::RemoveSlaveNode* remove_slave_node = request.add_remove_slave_node();
  InnerMessage::Node* node = remove_slave_node->mutable_node();
  node->set_ip(local_ip);
  node->set_port(g_pika_server->port());

  InnerMessage::Slot* db = remove_slave_node->mutable_slot();
  db->set_db_name(db_name);
  /*
   * Since the slot field is written in protobuffer,
   * slot_id is set to the default value 0 for compatibility
   * with older versions, but slot_id is not used
   */
  db->set_slot_id(0);

  std::string to_send;
  if (!request.SerializeToString(&to_send)) {
    LOG(WARNING) << "Serialize Remove Slave Node Failed, to Master (" << ip << ":" << port << "), " << db_name;
    return Status::Corruption("Serialize Failed");
  }
  return client_thread_->Write(ip, static_cast<int32_t>(port + kPortShiftReplServer), to_send);
}
