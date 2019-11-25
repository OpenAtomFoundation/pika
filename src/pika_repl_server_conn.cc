// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       std::string ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

PikaReplServerConn::~PikaReplServerConn() {
}

void PikaReplServerConn::HandleMetaSyncRequest(void* arg) {
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
    LOG(INFO) << "Receive MetaSync, Slave ip: " << node.ip() << ", Slave port:"
      << node.port();
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

void PikaReplServerConn::HandleTrySyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::TrySync try_sync_request = req->try_sync();
  InnerMessage::Partition partition_request = try_sync_request.partition();
  InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();
  InnerMessage::Node node = try_sync_request.node();

  InnerMessage::InnerResponse response;
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  InnerMessage::BinlogOffset* master_partition_boffset = try_sync_response->mutable_binlog_offset();

  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();

  bool pre_success = true;
  response.set_type(InnerMessage::Type::kTrySync);
  std::shared_ptr<SyncMasterPartition> partition =
    g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!partition) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Partition not found");
    LOG(WARNING) << "Table Name: " << table_name << " Partition ID: "
      << partition_id << " Not Found, TrySync Error";
    pre_success = false;
  }

  BinlogOffset boffset;
  std::string partition_name;
  if (pre_success) {
    partition_name = partition->PartitionName();
    LOG(INFO) << "Receive Trysync, Slave ip: " << node.ip() << ", Slave port:"
      << node.port() << ", Partition: " << partition_name << ", filenum: "
      << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();

    response.set_code(InnerMessage::kOk);
    partition_response->set_table_name(table_name);
    partition_response->set_partition_id(partition_id);
    Status s = partition->Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
    if (!s.ok()) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Handle TrySync, Partition: "
        << partition_name << " Get binlog offset error, TrySync failed";
      pre_success = false;
    }
  }

  if (pre_success) {
    master_partition_boffset->set_filenum(boffset.filenum);
    master_partition_boffset->set_offset(boffset.offset);
    if (boffset.filenum < slave_boffset.filenum()
      || (boffset.filenum == slave_boffset.filenum() && boffset.offset < slave_boffset.offset())) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
      LOG(WARNING) << "Slave offset is larger than mine, Slave ip: "
        << node.ip() << ", Slave port: " << node.port() << ", Partition: "
        << partition_name << ", filenum: " << slave_boffset.filenum()
        << ", pro_offset_: " << slave_boffset.offset();
      pre_success = false;
    }
    if (pre_success) {
      std::string confile = NewFileName(partition->Logger()->filename(), slave_boffset.filenum());
      if (!slash::FileExists(confile)) {
        LOG(INFO) << "Partition: " << partition_name << " binlog has been purged, may need full sync";
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
        pre_success = false;
      }
    }
    if (pre_success) {
      PikaBinlogReader reader;
      reader.Seek(partition->Logger(), slave_boffset.filenum(), slave_boffset.offset());
      BinlogOffset seeked_offset;
      reader.GetReaderStatus(&(seeked_offset.filenum), &(seeked_offset.offset));
      if (seeked_offset.filenum != slave_boffset.filenum() || seeked_offset.offset != slave_boffset.offset()) {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
        LOG(WARNING) << "Slave offset is not a start point of cur log, Slave ip: "
          << node.ip() << ", Slave port: " << node.port() << ", Partition: "
          << partition_name << ", cloest start point, filenum: " << seeked_offset.filenum
          << ", offset: " << seeked_offset.offset;
        pre_success = false;
      }
    }
  }

  if (pre_success) {
    if (!partition->CheckSlaveNodeExist(node.ip(), node.port())) {
      int32_t session_id = partition->GenSessionId();
      if (session_id != -1) {
        try_sync_response->set_session_id(session_id);
        // incremental sync
        Status s = partition->AddSlaveNode(node.ip(), node.port(), session_id);
        if (!s.ok()) {
          try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
          LOG(WARNING) << "Partition: " << partition_name << " TrySync Failed, " << s.ToString();
          pre_success = false;
        } else {
          const std::string ip_port = slash::IpPortString(node.ip(), node.port());
          g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
          try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
          LOG(INFO) << "Partition: " << partition_name << " TrySync Success, Session: " << session_id;
        }
      } else {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
        LOG(WARNING) << "Partition: " << partition_name << ", Gen Session id Failed";
        pre_success = false;
      }
    } else {
      int32_t session_id;
      Status s = partition->GetSlaveNodeSession(node.ip(), node.port(), &session_id);
      if (!s.ok()) {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
        LOG(WARNING) << "Partition: " << partition_name << ", Get Session id Failed" << s.ToString();
        pre_success = false;
      } else {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
        try_sync_response->set_session_id(session_id);
        LOG(INFO) << "Partition: " << partition_name << " TrySync Success, Session: " << session_id;
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


void PikaReplServerConn::HandleDBSyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::DBSync db_sync_request = req->db_sync();
  InnerMessage::Partition partition_request = db_sync_request.partition();
  InnerMessage::Node node = db_sync_request.node();
  InnerMessage::BinlogOffset slave_boffset = db_sync_request.binlog_offset();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  std::string partition_name = table_name + "_" + std::to_string(partition_id);

  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kDBSync);
  InnerMessage::InnerResponse::DBSync* db_sync_response = response.mutable_db_sync();
  InnerMessage::Partition* partition_response = db_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  LOG(INFO) << "Handle partition DBSync Request";
  bool prior_success = true;
  std::shared_ptr<SyncMasterPartition> master_partition =
    g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!master_partition) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id
      << ", NotFound";
    prior_success = false;
  }
  if (prior_success) {
    if (!master_partition->CheckSlaveNodeExist(node.ip(), node.port())) {
      int32_t session_id = master_partition->GenSessionId();
      if (session_id == -1) {
        response.set_code(InnerMessage::kError);
        LOG(WARNING) << "Partition: " << partition_name << ", Gen Session id Failed";
        prior_success = false;
      }
      if (prior_success) {
        db_sync_response->set_session_id(session_id);
        Status s = master_partition->AddSlaveNode(node.ip(), node.port(), session_id);
        if (s.ok()) {
          const std::string ip_port = slash::IpPortString(node.ip(), node.port());
          g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
          LOG(INFO) << "Partition: " << partition_name << " Handle DBSync Request Success, Session: " << session_id;
        } else {
          response.set_code(InnerMessage::kError);
          LOG(WARNING) << "Partition: " << partition_name << " Handle DBSync Request Failed, " << s.ToString();
          prior_success = false;
        }
      } else {
        db_sync_response->set_session_id(-1);
      }
    } else {
      int32_t session_id;
      Status s = master_partition->GetSlaveNodeSession(node.ip(), node.port(), &session_id);
      if (!s.ok()) {
        response.set_code(InnerMessage::kError);
        LOG(WARNING) << "Partition: " << partition_name << ", Get Session id Failed" << s.ToString();
        prior_success = false;
        db_sync_response->set_session_id(-1);
      } else {
        db_sync_response->set_session_id(session_id);
        LOG(INFO) << "Partition: " << partition_name << " Handle DBSync Request Success, Session: " << session_id;
      }
    }
  }

  g_pika_server->TryDBSync(node.ip(), node.port() + kPortShiftRSync,
      table_name, partition_id, slave_boffset.filenum());

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

void PikaReplServerConn::HandleBinlogSyncRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  if (!req->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    //conn->NotifyClose();
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

  std::shared_ptr<SyncMasterPartition> master_partition
    = g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!master_partition) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id
      << ", NotFound";
    delete task_arg;
    return;
  }

  if (!master_partition->CheckSessionId(node.ip(), node.port(),
        table_name, partition_id, session_id)) {
    LOG(WARNING) << "Check Session failed " << node.ip() << ":" << node.port()
        << ", " << table_name << "_" << partition_id;
    //conn->NotifyClose();
    delete task_arg;
    return;
  }

  // Set ack info from slave
  RmNode slave_node = RmNode(node.ip(), node.port(), table_name, partition_id);

  Status s = master_partition->SetLastRecvTime(node.ip(), node.port(), slash::NowMicros());
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

    Status s = master_partition->ActivateSlaveBinlogSync(node.ip(), node.port(), range_start);
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

  master_partition->ConsistencyScheduleApplyLog();

  delete task_arg;
  g_pika_server->SignalAuxiliary();
  return;
}

void PikaReplServerConn::HandleRemoveSlaveNodeRequest(void* arg) {
  ReplServerTaskArg* task_arg = static_cast<ReplServerTaskArg*>(arg);
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  if (!req->remove_slave_node_size()) {
    LOG(WARNING) << "Pb parse error";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  const InnerMessage::InnerRequest::RemoveSlaveNode& remove_slave_node_req = req->remove_slave_node(0);
  const InnerMessage::Node& node = remove_slave_node_req.node();
  const InnerMessage::Partition& partition = remove_slave_node_req.partition();

  std::string table_name = partition.table_name();
  uint32_t partition_id = partition.partition_id();
  std::shared_ptr<SyncMasterPartition> master_partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!master_partition) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id
        << ", NotFound";
  }
  Status s = master_partition->RemoveSlaveNode(node.ip(), node.port());

  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::kOk);
  response.set_type(InnerMessage::Type::kRemoveSlaveNode);
  InnerMessage::InnerResponse::RemoveSlaveNode* remove_slave_node_response = response.add_remove_slave_node();
  InnerMessage::Partition* partition_response = remove_slave_node_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);
  InnerMessage::Node* node_response = remove_slave_node_response->mutable_node();
  node_response->set_ip(g_pika_server->host());
  node_response->set_port(g_pika_server->port());

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || conn->WriteResp(reply_str)) {
    LOG(WARNING) << "Remove Slave Node Failed";
    conn->NotifyClose();
    delete task_arg;
    return;
  }
  conn->NotifyWrite();
  delete task_arg;
}

int PikaReplServerConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerRequest> req = std::make_shared<InnerMessage::InnerRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  switch (req->type()) {
    case InnerMessage::kMetaSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleMetaSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kTrySync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleTrySyncRequest, task_arg);
      break;
    }
    case InnerMessage::kDBSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleDBSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleBinlogSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kRemoveSlaveNode:
    {
      ReplServerTaskArg* task_arg = new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleRemoveSlaveNodeRequest, task_arg);
      break;
    }
    default:
      break;
  }
  return 0;
}
