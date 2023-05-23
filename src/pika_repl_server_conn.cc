// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"

using pstd::Status;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplServerConn::PikaReplServerConn(int fd, const std::string& ip_port, net::Thread* thread, void* worker_specific_data,
                                       net::NetMultiplexer* mpx)
    : PbConn(fd, ip_port, thread, mpx) {}

PikaReplServerConn::~PikaReplServerConn() = default;

void PikaReplServerConn::HandleMetaSyncRequest(void* arg) {
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::MetaSync meta_sync_request = req->meta_sync();
  const InnerMessage::Node& node = meta_sync_request.node();
  std::string masterauth = meta_sync_request.has_auth() ? meta_sync_request.auth() : "";

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::kMetaSync);
  if (!g_pika_conf->requirepass().empty() && g_pika_conf->requirepass() != masterauth) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Auth with master error, Invalid masterauth");
  } else {
    LOG(INFO) << "Receive MetaSync, Slave ip: " << node.ip() << ", Slave port:" << node.port();
    std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
    bool success = g_pika_server->TryAddSlave(node.ip(), node.port(), conn->fd(), table_structs);
    const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
    g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
    if (!success) {
      response.set_code(InnerMessage::kOther);
      response.set_reply("Slave AlreadyExist");
    } else {
      g_pika_server->BecomeMaster();
      response.set_code(InnerMessage::kOk);
      InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
      for (const auto& table_struct : table_structs) {
        InnerMessage::InnerResponse_MetaSync_TableInfo* table_info = meta_sync->add_tables_info();
        table_info->set_table_name(table_struct.table_name);
        table_info->set_partition_num(table_struct.partition_num);
      }
    }
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Process MetaSync request serialization failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

void PikaReplServerConn::HandleTrySyncRequest(void* arg) {
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::TrySync try_sync_request = req->try_sync();
  const InnerMessage::Partition& partition_request = try_sync_request.partition();
  const InnerMessage::BinlogOffset& slave_boffset = try_sync_request.binlog_offset();
  const InnerMessage::Node& node = try_sync_request.node();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  std::string partition_name;

  InnerMessage::InnerResponse response;
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);

  bool pre_success = true;
  response.set_type(InnerMessage::Type::kTrySync);
  std::shared_ptr<SyncMasterPartition> partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!partition) {
    response.set_code(InnerMessage::kError);
    response.set_reply("Partition not found");
    LOG(WARNING) << "Table Name: " << table_name << " Partition ID: " << partition_id << " Not Found, TrySync Error";
    pre_success = false;
  } else {
    partition_name = partition->PartitionName();
    LOG(INFO) << "Receive Trysync, Slave ip: " << node.ip() << ", Slave port:" << node.port()
              << ", Partition: " << partition_name << ", filenum: " << slave_boffset.filenum()
              << ", pro_offset: " << slave_boffset.offset();
    response.set_code(InnerMessage::kOk);
  }

  if (pre_success && req->has_consensus_meta()) {
    if (partition->GetNumberOfSlaveNode() >= g_pika_conf->replication_num() &&
        !partition->CheckSlaveNodeExist(node.ip(), node.port())) {
      LOG(WARNING) << "Current replication num: " << partition->GetNumberOfSlaveNode()
                   << " hits configuration replication-num " << g_pika_conf->replication_num() << " stop trysync.";
      pre_success = false;
    }
    if (pre_success) {
      const InnerMessage::ConsensusMeta& meta = req->consensus_meta();
      // need to response to outdated pb, new follower count on this response to update term
      if (meta.term() > partition->ConsensusTerm()) {
        LOG(INFO) << "Update " << partition_name << " term from " << partition->ConsensusTerm() << " to "
                  << meta.term();
        partition->ConsensusUpdateTerm(meta.term());
      }
    }
    if (pre_success) {
      pre_success = TrySyncConsensusOffsetCheck(partition, req->consensus_meta(), &response, try_sync_response);
    }
  } else if (pre_success) {
    pre_success = TrySyncOffsetCheck(partition, try_sync_request, try_sync_response);
  }

  if (pre_success) {
    pre_success = TrySyncUpdateSlaveNode(partition, try_sync_request, conn, try_sync_response);
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Handle Try Sync Failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

bool PikaReplServerConn::TrySyncUpdateSlaveNode(const std::shared_ptr<SyncMasterPartition>& partition,
                                                const InnerMessage::InnerRequest::TrySync& try_sync_request,
                                                const std::shared_ptr<net::PbConn>& conn,
                                                InnerMessage::InnerResponse::TrySync* try_sync_response) {
  const InnerMessage::Node& node = try_sync_request.node();
  std::string partition_name = partition->PartitionName();

  if (!partition->CheckSlaveNodeExist(node.ip(), node.port())) {
    int32_t session_id = partition->GenSessionId();
    if (session_id == -1) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Partition: " << partition_name << ", Gen Session id Failed";
      return false;
    }
    try_sync_response->set_session_id(session_id);
    // incremental sync
    Status s = partition->AddSlaveNode(node.ip(), node.port(), session_id);
    if (!s.ok()) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Partition: " << partition_name << " TrySync Failed, " << s.ToString();
      return false;
    }
    const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
    g_pika_rm->ReplServerUpdateClientConnMap(ip_port, conn->fd());
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
    LOG(INFO) << "Partition: " << partition_name << " TrySync Success, Session: " << session_id;
  } else {
    int32_t session_id;
    Status s = partition->GetSlaveNodeSession(node.ip(), node.port(), &session_id);
    if (!s.ok()) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Partition: " << partition_name << ", Get Session id Failed" << s.ToString();
      return false;
    }
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
    try_sync_response->set_session_id(session_id);
    LOG(INFO) << "Partition: " << partition_name << " TrySync Success, Session: " << session_id;
  }
  return true;
}

bool PikaReplServerConn::TrySyncConsensusOffsetCheck(const std::shared_ptr<SyncMasterPartition>& partition,
                                                     const InnerMessage::ConsensusMeta& meta,
                                                     InnerMessage::InnerResponse* response,
                                                     InnerMessage::InnerResponse::TrySync* try_sync_response) {
  LogOffset last_log_offset;
  last_log_offset.b_offset.filenum = meta.log_offset().filenum();
  last_log_offset.b_offset.offset = meta.log_offset().offset();
  last_log_offset.l_offset.term = meta.log_offset().term();
  last_log_offset.l_offset.index = meta.log_offset().index();
  std::string partition_name = partition->PartitionName();
  bool reject = false;
  std::vector<LogOffset> hints;
  Status s = partition->ConsensusLeaderNegotiate(last_log_offset, &reject, &hints);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      LOG(INFO) << "Partition: " << partition_name << " need full sync";
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
      return false;
    } else {
      LOG(WARNING) << "Partition:" << partition_name << " error " << s.ToString();
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      return false;
    }
  }
  try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
  uint32_t term = partition->ConsensusTerm();
  BuildConsensusMeta(reject, hints, term, response);
  return !reject;
}

bool PikaReplServerConn::TrySyncOffsetCheck(const std::shared_ptr<SyncMasterPartition>& partition,
                                            const InnerMessage::InnerRequest::TrySync& try_sync_request,
                                            InnerMessage::InnerResponse::TrySync* try_sync_response) {
  const InnerMessage::Node& node = try_sync_request.node();
  const InnerMessage::BinlogOffset& slave_boffset = try_sync_request.binlog_offset();
  std::string partition_name = partition->PartitionName();

  BinlogOffset boffset;
  Status s = partition->Logger()->GetProducerStatus(&(boffset.filenum), &(boffset.offset));
  if (!s.ok()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Handle TrySync, Partition: " << partition_name << " Get binlog offset error, TrySync failed";
    return false;
  }
  InnerMessage::BinlogOffset* master_partition_boffset = try_sync_response->mutable_binlog_offset();
  master_partition_boffset->set_filenum(boffset.filenum);
  master_partition_boffset->set_offset(boffset.offset);

  if (boffset.filenum < slave_boffset.filenum() ||
      (boffset.filenum == slave_boffset.filenum() && boffset.offset < slave_boffset.offset())) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointLarger);
    LOG(WARNING) << "Slave offset is larger than mine, Slave ip: " << node.ip() << ", Slave port: " << node.port()
                 << ", Partition: " << partition_name << ", filenum: " << slave_boffset.filenum()
                 << ", pro_offset_: " << slave_boffset.offset();
    return false;
  }

  std::string confile = NewFileName(partition->Logger()->filename(), slave_boffset.filenum());
  if (!pstd::FileExists(confile)) {
    LOG(INFO) << "Partition: " << partition_name << " binlog has been purged, may need full sync";
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kSyncPointBePurged);
    return false;
  }

  PikaBinlogReader reader;
  reader.Seek(partition->Logger(), slave_boffset.filenum(), slave_boffset.offset());
  BinlogOffset seeked_offset;
  reader.GetReaderStatus(&(seeked_offset.filenum), &(seeked_offset.offset));
  if (seeked_offset.filenum != slave_boffset.filenum() || seeked_offset.offset != slave_boffset.offset()) {
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
    LOG(WARNING) << "Slave offset is not a start point of cur log, Slave ip: " << node.ip()
                 << ", Slave port: " << node.port() << ", Partition: " << partition_name
                 << ", cloest start point, filenum: " << seeked_offset.filenum << ", offset: " << seeked_offset.offset;
    return false;
  }
  return true;
}

void PikaReplServerConn::BuildConsensusMeta(const bool& reject, const std::vector<LogOffset>& hints,
                                            const uint32_t& term, InnerMessage::InnerResponse* response) {
  InnerMessage::ConsensusMeta* consensus_meta = response->mutable_consensus_meta();
  consensus_meta->set_term(term);
  consensus_meta->set_reject(reject);
  if (!reject) {
    return;
  }
  for (const auto& hint : hints) {
    InnerMessage::BinlogOffset* offset = consensus_meta->add_hint();
    offset->set_filenum(hint.b_offset.filenum);
    offset->set_offset(hint.b_offset.offset);
    offset->set_term(hint.l_offset.term);
    offset->set_index(hint.l_offset.index);
  }
}

void PikaReplServerConn::HandleDBSyncRequest(void* arg) {
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;

  InnerMessage::InnerRequest::DBSync db_sync_request = req->db_sync();
  const InnerMessage::Partition& partition_request = db_sync_request.partition();
  const InnerMessage::Node& node = db_sync_request.node();
  const InnerMessage::BinlogOffset& slave_boffset = db_sync_request.binlog_offset();
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
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id << ", NotFound";
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
          const std::string ip_port = pstd::IpPortString(node.ip(), node.port());
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

  g_pika_server->TryDBSync(node.ip(), node.port() + kPortShiftRSync, table_name, partition_id, slave_boffset.filenum());

  std::string reply_str;
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Handle DBSync Failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

void PikaReplServerConn::HandleBinlogSyncRequest(void* arg) {
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  if (!req->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    // conn->NotifyClose();
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
  BinlogOffset b_range_start(ack_range_start.filenum(), ack_range_start.offset());
  BinlogOffset b_range_end(ack_range_end.filenum(), ack_range_end.offset());
  LogicOffset l_range_start(ack_range_start.term(), ack_range_start.index());
  LogicOffset l_range_end(ack_range_end.term(), ack_range_end.index());
  LogOffset range_start(b_range_start, l_range_start);
  LogOffset range_end(b_range_end, l_range_end);

  std::shared_ptr<SyncMasterPartition> master_partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!master_partition) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id << ", NotFound";
    return;
  }

  if (req->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = req->consensus_meta();
    if (meta.term() > master_partition->ConsensusTerm()) {
      LOG(INFO) << "Update " << table_name << ":" << partition_id << " term from " << master_partition->ConsensusTerm()
                << " to " << meta.term();
      master_partition->ConsensusUpdateTerm(meta.term());
    } else if (meta.term() < master_partition->ConsensusTerm()) /*outdated pb*/ {
      LOG(WARNING) << "Drop outdated binlog sync req " << table_name << ":" << partition_id
                   << " recv term: " << meta.term() << " local term: " << master_partition->ConsensusTerm();
      return;
    }
  }

  if (!master_partition->CheckSessionId(node.ip(), node.port(), table_name, partition_id, session_id)) {
    LOG(WARNING) << "Check Session failed " << node.ip() << ":" << node.port() << ", " << table_name << "_"
                 << partition_id;
    // conn->NotifyClose();
    return;
  }

  // Set ack info from slave
  RmNode slave_node = RmNode(node.ip(), node.port(), table_name, partition_id);

  Status s = master_partition->SetLastRecvTime(node.ip(), node.port(), pstd::NowMicros());
  if (!s.ok()) {
    LOG(WARNING) << "SetMasterLastRecvTime failed " << node.ip() << ":" << node.port() << ", " << table_name << "_"
                 << partition_id << " " << s.ToString();
    conn->NotifyClose();
    return;
  }

  if (is_first_send) {
    if (range_start.b_offset != range_end.b_offset) {
      LOG(WARNING) << "first binlogsync request pb argument invalid";
      conn->NotifyClose();
      return;
    }

    Status s = master_partition->ActivateSlaveBinlogSync(node.ip(), node.port(), range_start);
    if (!s.ok()) {
      LOG(WARNING) << "Activate Binlog Sync failed " << slave_node.ToString() << " " << s.ToString();
      conn->NotifyClose();
      return;
    }
    return;
  }

  // not the first_send the range_ack cant be 0
  // set this case as ping
  if (range_start.b_offset == BinlogOffset() && range_end.b_offset == BinlogOffset()) {
    return;
  }
  s = g_pika_rm->UpdateSyncBinlogStatus(slave_node, range_start, range_end);
  if (!s.ok()) {
    LOG(WARNING) << "Update binlog ack failed " << table_name << " " << partition_id << " " << s.ToString();
    conn->NotifyClose();
    return;
  }

  g_pika_server->SignalAuxiliary();
}

void PikaReplServerConn::HandleRemoveSlaveNodeRequest(void* arg) {
  std::unique_ptr<ReplServerTaskArg> task_arg(static_cast<ReplServerTaskArg*>(arg));
  const std::shared_ptr<InnerMessage::InnerRequest> req = task_arg->req;
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  if (req->remove_slave_node_size() == 0) {
    LOG(WARNING) << "Pb parse error";
    conn->NotifyClose();
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
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id << ", NotFound";
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
  if (!response.SerializeToString(&reply_str) || (conn->WriteResp(reply_str) != 0)) {
    LOG(WARNING) << "Remove Slave Node Failed";
    conn->NotifyClose();
    return;
  }
  conn->NotifyWrite();
}

int PikaReplServerConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerRequest> req = std::make_shared<InnerMessage::InnerRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  switch (req->type()) {
    case InnerMessage::kMetaSync: {
      auto task_arg =
          new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleMetaSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kTrySync: {
      auto task_arg =
          new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleTrySyncRequest, task_arg);
      break;
    }
    case InnerMessage::kDBSync: {
      auto task_arg =
          new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleDBSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kBinlogSync: {
      auto task_arg =
          new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleBinlogSyncRequest, task_arg);
      break;
    }
    case InnerMessage::kRemoveSlaveNode: {
      auto task_arg =
          new ReplServerTaskArg(req, std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()));
      g_pika_rm->ScheduleReplServerBGTask(&PikaReplServerConn::HandleRemoveSlaveNodeRequest, task_arg);
      break;
    }
    default:
      break;
  }
  return 0;
}
