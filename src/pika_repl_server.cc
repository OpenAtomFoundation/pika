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
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kBinlogSync);
  for (const auto task :tasks) {
    InnerMessage::InnerResponse::BinlogSync* binlog_sync = response.add_binlog_sync();
    binlog_sync->set_session_id(task.rm_node_.SessionId());
    InnerMessage::Partition* partition = binlog_sync->mutable_partition();
    partition->set_table_name(task.rm_node_.TableName());
    partition->set_partition_id(task.rm_node_.PartitionId());
    InnerMessage::BinlogOffset* boffset = binlog_sync->mutable_binlog_offset();
    boffset->set_filenum(task.binlog_chip_.offset_.filenum);
    boffset->set_offset(task.binlog_chip_.offset_.offset);
    binlog_sync->set_binlog(task.binlog_chip_.binlog_);
  }

  std::string binlog_chip_pb;
  if (!response.SerializeToString(&binlog_chip_pb)) {
    return Status::Corruption("Serialized Failed");
  }
  return Write(ip, port, binlog_chip_pb);
}

slash::Status PikaReplServer::Write(const std::string& ip,
                                    const int port,
                                    const std::string& msg) {
  slash::RWLock l(&client_conn_rwlock_, false);
  const std::string ip_port = slash::IpPortString(ip, port);
  if (client_conn_map.find(ip_port) == client_conn_map.end()) {
    return Status::NotFound("The " + ip_port + " fd cannot be found");
  }
  int fd = client_conn_map[ip_port];
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
  client_conn_map[ip_port] = fd;
}

void PikaReplServer::RemoveClientConn(int fd) {
  slash::RWLock l(&client_conn_rwlock_, true);
  std::map<std::string, int>::const_iterator iter = client_conn_map.begin();
  while (iter != client_conn_map.end()) {
    if (iter->second == fd) {
      iter = client_conn_map.erase(iter);
      break;
    }
    iter++;
  }
}

void PikaReplServer::KillAllConns() {
  return pika_repl_server_thread_->KillAllConns();
}

void PikaReplServer::HandleMetaSyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::MetaSync meta_sync_request = req->meta_sync();
  InnerMessage::Node node = meta_sync_request.node();
  std::string masterauth = meta_sync_request.has_auth() ? meta_sync_request.auth() : "";

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::kMetaSync);
  if (!g_pika_conf->requirepass().empty()
    && g_pika_conf->requirepass() != masterauth) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Auth with master error, Invalid masterauth");
  } else {
    std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
    bool success = g_pika_server->TryAddSlave(node.ip(), node.port(), conn->fd(), table_structs);
    const std::string ip_port = slash::IpPortString(node.ip(), node.port());
    g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
    if (!success) {
      response.set_code(InnerMessage::kError);
      response.set_reply("Slave AlreadyExist");
    } else {
      g_pika_server->BecomeMaster();
      response.set_code(InnerMessage::kOk);
      InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
      meta_sync->set_classic_mode(g_pika_conf->classic_mode());
      for (const auto& table_struct : table_structs) {
        InnerMessage::InnerResponse_MetaSync_TableInfo* table_info = meta_sync->add_tables_info();
        table_info->set_table_name(table_struct.table_name);
        table_info->set_partition_num(table_struct.partition_num);
      }
    }
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || conn->WriteResp(reply_str)) {
    LOG(WARNING) << "Process MetaSync request serialization failed";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  conn->NotifyWrite();
  delete task_arg;
}

void PikaReplServer::HandleTrySyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::TrySync try_sync_request = req->try_sync();
  InnerMessage::Partition partition_request = try_sync_request.partition();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::Type::kTrySync);
  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Partition not found");
    LOG(WARNING) << "Table Name: " << table_name << " Partition ID: "
      << partition_id << " Not Found, TrySync Error";
  } else {
    std::string partition_name = partition->GetPartitionName();
    InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();
    InnerMessage::Node node = try_sync_request.node();
    LOG(INFO) << "Receive Trysync, Slave ip: " << node.ip() << ", Slave port:"
      << node.port() << ", Partition: " << partition_name << ", filenum: "
      << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();

    response.set_code(InnerMessage::kOk);
    InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
    InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
    InnerMessage::BinlogOffset* master_partition_boffset = try_sync_response->mutable_binlog_offset();
    partition_response->set_table_name(table_name);
    partition_response->set_partition_id(partition_id);
    BinlogOffset boffset;
    if (!partition->GetBinlogOffset(&boffset)) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Handle TrySync, Partition: "
        << partition_name << " Get binlog offset error, TrySync failed";
    } else {
      master_partition_boffset->set_filenum(boffset.filenum);
      master_partition_boffset->set_offset(boffset.offset);
      if (boffset.filenum < slave_boffset.filenum()
        || (boffset.filenum == slave_boffset.filenum() && boffset.offset < slave_boffset.offset())) {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
        LOG(WARNING) << "Slave offset is larger than mine, Slave ip: "
          << node.ip() << ", Slave port: " << node.port() << ", Partition: "
          << partition_name << ", filenum: " << slave_boffset.filenum()
          << ", pro_offset_: " << slave_boffset.offset();
      } else {
        std::string confile = NewFileName(partition->logger()->filename, slave_boffset.filenum());
        if (!slash::FileExists(confile)) {
          LOG(INFO) << "Partition: " << partition_name << " binlog has been purged, may need full sync";
          try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
        } else {
          int32_t session_id = g_pika_rm->GenPartitionSessionId(table_name, partition_id);
          if (session_id != -1) {
            try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
            try_sync_response->set_session_id(session_id);
            // incremental sync
            Status s = g_pika_rm->AddPartitionSlave(RmNode(node.ip(), node.port(), table_name, partition_id, session_id));
            if (s.ok()) {
              LOG(INFO) << "Partition: " << partition_name << " TrySync Success";
            } else {
              LOG(WARNING) << "Partition: " << partition_name << " TrySync Failed, " << s.ToString();
            }
          } else {
            try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
            LOG(WARNING) << "Partition: " << partition_name << ", Gen Session id Failed";
          }
        }
      }
    }
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || conn->WriteResp(reply_str)) {
    LOG(WARNING) << "Handle Try Sync Failed";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  conn->NotifyWrite();
  delete task_arg;
}

void PikaReplServer::HandleDBSyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::DBSync db_sync_request = req->db_sync();
  InnerMessage::Partition partition_request = db_sync_request.partition();
  InnerMessage::Node node = db_sync_request.node();
  InnerMessage::BinlogOffset slave_boffset = db_sync_request.binlog_offset();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();

  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kDBSync);
  InnerMessage::InnerResponse::DBSync* db_sync_response = response.mutable_db_sync();
  InnerMessage::Partition* partition_response = db_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  LOG(INFO) << "Handle partition DBSync Request";
  g_pika_server->TryDBSync(node.ip(), node.port() + kPortShiftRSync,
      table_name, partition_id, slave_boffset.filenum());
  db_sync_response->set_reply_code(InnerMessage::InnerResponse::DBSync::kWait);

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || conn->WriteResp(reply_str)) {
    LOG(WARNING) << "Handle DBSync Failed";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  conn->NotifyWrite();
  delete task_arg;
}

void PikaReplServer::HandleBinlogSyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  if (!req->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  const InnerMessage::InnerRequest::BinlogSync& binlog_req = req->binlog_sync();
  const InnerMessage::Node& node = binlog_req.node();
  const std::string& table_name = binlog_req.table_name();
  uint32_t partition_id = binlog_req.partition_id();

  bool is_first_send = binlog_req.first_send();
  int32_t session_id = binlog_req.session_id();
  const InnerMessage::BinlogOffset& ack_range_start = binlog_req.ack_range_start();
  const InnerMessage::BinlogOffset& ack_range_end = binlog_req.ack_range_end();
  BinlogOffset range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset range_end(ack_range_end.filenum(), ack_range_end.offset());

  if (!g_pika_rm->CheckMasterPartitionSessionId(node.ip(),
              node.port(), table_name, partition_id, session_id)) {
    LOG(WARNING) << "Check Session failed " << node.ip() << ":" << node.port()
        << ", " << table_name << "_" << partition_id;
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  // Set ack info from slave
  RmNode slave_node = RmNode(node.ip(), node.port(), table_name, partition_id);

  Status s = g_pika_rm->SetMasterLastRecvTime(slave_node, slash::NowMicros());
  if (!s.ok()) {
    LOG(WARNING) << "SetMasterLastRecvTime failed " << node.ip() << ":" << node.port()
        << ", " << table_name << "_" << partition_id << " " << s.ToString();
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  if (is_first_send) {
    if (!(range_start == range_end)) {
      LOG(WARNING) << "first binlogsync request pb argument invalid";
      conn->NotifyClose();
      delete task_arg;
      return;
    }
    Status s = g_pika_rm->ActivateBinlogSync(slave_node, range_start);
    if (!s.ok()) {
      LOG(WARNING) << "Activate Binlog Sync failed " << slave_node.ToString() << " " << s.ToString();
      conn->NotifyClose();
      delete task_arg;
      return;
    }
    delete task_arg;
    return;
  }

  // not the first_send the range_ack cant be 0
  // set this case as ping
  if (range_start == BinlogOffset() && range_end == BinlogOffset()) {
    delete task_arg;
    return;
  }
  s = g_pika_rm->UpdateSyncBinlogStatus(slave_node, range_start, range_end);
  if (!s.ok()) {
    LOG(WARNING) << "Update binlog ack failed " << table_name << " " << partition_id << " " << s.ToString();
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  delete task_arg;
  g_pika_server->SignalAuxiliary();
  return;
}
