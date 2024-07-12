// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <sys/time.h>

#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "pstd/include/pstd_string.h"
#include "pika_inner_message.pb.h"

using pstd::Status;

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

PikaReplClientConn::PikaReplClientConn(int fd, const std::string& ip_port, net::Thread* thread,
                                       void* worker_specific_data, net::NetMultiplexer* mpx)
    : net::PbConn(fd, ip_port, thread, mpx) {}

bool PikaReplClientConn::IsDBStructConsistent(const std::vector<DBStruct>& current_dbs,
                                                 const std::vector<DBStruct>& expect_dbs) {
  if (current_dbs.size() != expect_dbs.size()) {
    return false;
  }
  for (const auto& db_struct : current_dbs) {
    if (find(expect_dbs.begin(), expect_dbs.end(), db_struct) == expect_dbs.end()) {
      return false;
    }
  }
  return true;
}

int PikaReplClientConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerResponse> response = std::make_shared<InnerMessage::InnerResponse>();
  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, static_cast<int32_t>(header_len_));
  ::google::protobuf::io::CodedInputStream decoder(&input);
  decoder.SetTotalBytesLimit(g_pika_conf->max_conn_rbuf_size());
  bool success = response->ParseFromCodedStream(&decoder) && decoder.ConsumedEntireMessage();
  if (!success) {
    LOG(WARNING) << "ParseFromArray FAILED! "
                 << " msg_len: " << header_len_;
    g_pika_server->SyncError();
    return -1;
  }
  switch (response->type()) {
    case InnerMessage::kMetaSync: {
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleMetaSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kDBSync: {
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleDBSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kTrySync: {
      const std::string& db_name = response->try_sync().slot().db_name();
      //TrySync resp must contain db_name
      assert(!db_name.empty());
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTaskByDBName(&PikaReplClientConn::HandleTrySyncResponse, static_cast<void*>(task_arg), db_name);
      break;
    }
    case InnerMessage::kBinlogSync: {
      DispatchBinlogRes(response);
      break;
    }
    case InnerMessage::kRemoveSlaveNode: {
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleRemoveSlaveNodeResponse,
                                          static_cast<void*>(task_arg));
      break;
    }
    default:
      break;
  }
  return 0;
}

void PikaReplClientConn::HandleMetaSyncResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  if (response->code() == InnerMessage::kOther) {
    std::string reply = response->has_reply() ? response->reply() : "";
    // keep sending MetaSync
    LOG(WARNING) << "Meta Sync Failed: " << reply << " will keep sending MetaSync msg";
    return;
  }

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply;
    g_pika_server->SyncError();
    conn->NotifyClose();
    return;
  }

  const InnerMessage::InnerResponse_MetaSync meta_sync = response->meta_sync();

  std::vector<DBStruct> master_db_structs;
  for (int idx = 0; idx < meta_sync.dbs_info_size(); ++idx) {
    const InnerMessage::InnerResponse_MetaSync_DBInfo& db_info = meta_sync.dbs_info(idx);
    master_db_structs.push_back({db_info.db_name()});
  }

  std::vector<DBStruct> self_db_structs = g_pika_conf->db_structs();
  if (!PikaReplClientConn::IsDBStructConsistent(self_db_structs, master_db_structs)) {
    LOG(WARNING) << "Self db structs(number of databases: " << self_db_structs.size()
                 << ") inconsistent with master(number of databases: " << master_db_structs.size()
                 << "), failed to establish master-slave relationship";
    g_pika_server->SyncError();
    conn->NotifyClose();
    return;
  }

  // The relicationid obtained from the server is null
  if (meta_sync.replication_id() == "") {
    LOG(WARNING) << "Meta Sync Failed: the relicationid obtained from the server is null, keep sending MetaSync msg";
    return;
  }

  // The Replicationids of both the primary and secondary Replicationid are not empty and are not equal
  if (g_pika_conf->replication_id() != meta_sync.replication_id() && g_pika_conf->replication_id() != "") {
    LOG(WARNING) << "Meta Sync Failed: replicationid on both sides of the connection are inconsistent";
    g_pika_server->SyncError();
    conn->NotifyClose();
    return;
  }

  // First synchronization between the master and slave
  if (g_pika_conf->replication_id() != meta_sync.replication_id()) {
    LOG(INFO) << "New node is added to the cluster and requires full replication, remote replication id: " << meta_sync.replication_id()
              << ", local replication id: " << g_pika_conf->replication_id();
    g_pika_server->force_full_sync_ = true;
    g_pika_conf->SetReplicationID(meta_sync.replication_id());
    g_pika_conf->ConfigRewriteReplicationID();
  }

  g_pika_conf->SetWriteBinlog("yes");
  g_pika_server->PrepareDBTrySync();
  g_pika_server->FinishMetaSync();
  LOG(INFO) << "Finish to handle meta sync response";
}

void PikaReplClientConn::HandleDBSyncResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();
  const InnerMessage::Slot& db_response = db_sync_response.slot();
  const std::string& db_name = db_response.db_name();

  std::shared_ptr<SyncSlaveDB> slave_db =
      g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "Slave DB: " << db_name << " Not Found";
    return;
  }

  if (response->code() != InnerMessage::kOk) {
    slave_db->SetReplState(ReplState::kError);
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    return;
  }

  slave_db->SetMasterSessionId(session_id);

  slave_db->StopRsync();
  slave_db->SetReplState(ReplState::kWaitDBSync);
  LOG(INFO) << "DB: " << db_name << " Need Wait To Sync";

  //now full sync is starting, add an unfinished full sync count
  g_pika_conf->AddInternalUsedUnfinishedFullSync(slave_db->DBName());
}

void PikaReplClientConn::HandleTrySyncResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "TrySync Failed: " << reply;
    return;
  }
  const InnerMessage::InnerResponse_TrySync& try_sync_response = response->try_sync();
  const InnerMessage::Slot& db_response = try_sync_response.slot();
  std::string db_name = db_response.db_name();
  std::shared_ptr<SyncMasterDB> db =
      g_pika_rm->GetSyncMasterDBByName(DBInfo(db_name));
  if (!db) {
    LOG(WARNING) << "DB: " << db_name << " Not Found";
    return;
  }

  std::shared_ptr<SyncSlaveDB> slave_db =
      g_pika_rm->GetSyncSlaveDBByName(DBInfo(db_name));
  if (!slave_db) {
    LOG(WARNING) << "DB: " << db_name << "Not Found";
    return;
  }

  LogicOffset logic_last_offset;
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    BinlogOffset boffset;
    int32_t session_id = try_sync_response.session_id();
    db->Logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);
    slave_db->SetMasterSessionId(session_id);
    LogOffset offset(boffset, logic_last_offset);
    g_pika_rm->SendBinlogSyncAckRequest(db_name, offset, offset, true);
    slave_db->SetReplState(ReplState::kConnected);
    // after connected, update receive time first to avoid connection timeout
    slave_db->SetLastRecvTime(pstd::NowMicros());

    LOG(INFO) << "DB: " << db_name << " TrySync Ok";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    slave_db->SetReplState(ReplState::kTryDBSync);
    LOG(INFO) << "DB: " << db_name << " Need To Try DBSync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    slave_db->SetReplState(ReplState::kError);
    LOG(WARNING) << "DB: " << db_name << " TrySync Error, Because the invalid filenum and offset";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    slave_db->SetReplState(ReplState::kError);
    LOG(WARNING) << "DB: " << db_name << " TrySync Error";
  }
}

void PikaReplClientConn::DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse>& res) {
  // db to a bunch of binlog chips
  std::unordered_map<DBInfo, std::vector<int>*, hash_db_info> par_binlog;
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);
    // hash key: db
    DBInfo p_info(binlog_res.slot().db_name());
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    par_binlog[p_info]->push_back(i);
  }

  std::shared_ptr<SyncSlaveDB> slave_db;
  for (auto& binlog_nums : par_binlog) {
    RmNode node(binlog_nums.first.db_name_);
    slave_db = g_pika_rm->GetSyncSlaveDBByName(
        DBInfo(binlog_nums.first.db_name_));
    if (!slave_db) {
      LOG(WARNING) << "Slave DB: " << binlog_nums.first.db_name_ << " not exist";
      break;
    }
    slave_db->SetLastRecvTime(pstd::NowMicros());
    g_pika_rm->ScheduleWriteBinlogTask(binlog_nums.first.db_name_, res,
                                       std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()),
                                       reinterpret_cast<void*>(binlog_nums.second));
  }
}

void PikaReplClientConn::HandleRemoveSlaveNodeResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;
  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Remove slave node Failed: " << reply;
    return;
  }
}
