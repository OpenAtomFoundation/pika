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

#include "include/pika_rm.h"
#include "include/pika_server.h"

#include "pika_inner_message.pb.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaReplClientConn::PikaReplClientConn(int fd, const std::string& ip_port, net::Thread* thread,
                                       void* worker_specific_data, net::NetMultiplexer* mpx)
    : net::PbConn(fd, ip_port, thread, mpx) {}

bool PikaReplClientConn::IsTableStructConsistent(const std::vector<TableStruct>& current_tables,
                                                 const std::vector<TableStruct>& expect_tables) {
  if (current_tables.size() != expect_tables.size()) {
    return false;
  }
  for (const auto& table_struct : current_tables) {
    if (find(expect_tables.begin(), expect_tables.end(), table_struct) == expect_tables.end()) {
      return false;
    }
  }
  return true;
}

int PikaReplClientConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerResponse> response = std::make_shared<InnerMessage::InnerResponse>();
  ::google::protobuf::io::ArrayInputStream input(rbuf_ + cur_pos_ - header_len_, header_len_);
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
      ReplClientTaskArg* task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleMetaSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kDBSync: {
      ReplClientTaskArg* task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleDBSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kTrySync: {
      ReplClientTaskArg* task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleTrySyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kBinlogSync: {
      DispatchBinlogRes(response);
      break;
    }
    case InnerMessage::kRemoveSlaveNode: {
      ReplClientTaskArg* task_arg =
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
  if (g_pika_conf->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (g_pika_conf->classic_mode() ? "classic" : "sharding") << " mode, but master in "
                 << (meta_sync.classic_mode() ? "classic" : "sharding")
                 << " mode, failed to establish master-slave relationship";
    g_pika_server->SyncError();
    conn->NotifyClose();
    return;
  }

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    InnerMessage::InnerResponse_MetaSync_TableInfo table_info = meta_sync.tables_info(idx);
    master_table_structs.push_back({table_info.table_name(), static_cast<uint32_t>(table_info.partition_num()), {0}});
  }

  std::vector<TableStruct> self_table_structs = g_pika_conf->table_structs();
  if (!PikaReplClientConn::IsTableStructConsistent(self_table_structs, master_table_structs)) {
    LOG(WARNING) << "Self table structs(number of databases: " << self_table_structs.size()
                 << ") inconsistent with master(number of databases: " << master_table_structs.size()
                 << "), failed to establish master-slave relationship";
    g_pika_server->SyncError();
    conn->NotifyClose();
    return;
  }

  g_pika_conf->SetWriteBinlog("yes");
  g_pika_server->PreparePartitionTrySync();
  g_pika_server->FinishMetaSync();
  LOG(INFO) << "Finish to handle meta sync response";
}

void PikaReplClientConn::HandleDBSyncResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();
  const InnerMessage::Partition partition_response = db_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id = partition_response.partition_id();

  std::shared_ptr<SyncSlavePartition> slave_partition =
      g_pika_rm->GetSyncSlavePartitionByName(PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table_name << ":" << partition_id << " Not Found";
    return;
  }

  if (response->code() != InnerMessage::kOk) {
    slave_partition->SetReplState(ReplState::kError);
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    return;
  }

  slave_partition->SetMasterSessionId(session_id);

  std::string partition_name = slave_partition->PartitionName();
  slave_partition->SetReplState(ReplState::kWaitDBSync);
  LOG(INFO) << "Partition: " << partition_name << " Need Wait To Sync";
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
  const InnerMessage::Partition& partition_response = try_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id = partition_response.partition_id();
  std::shared_ptr<SyncMasterPartition> partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!partition) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id << " Not Found";
    return;
  }

  std::shared_ptr<SyncSlavePartition> slave_partition =
      g_pika_rm->GetSyncSlavePartitionByName(PartitionInfo(table_name, partition_id));
  if (!slave_partition) {
    LOG(WARNING) << "Slave Partition: " << table_name << ":" << partition_id << " Not Found";
    return;
  }

  LogicOffset logic_last_offset;
  if (response->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = response->consensus_meta();
    if (meta.term() > partition->ConsensusTerm()) {
      LOG(INFO) << "Update " << table_name << ":" << partition_id << " term from " << partition->ConsensusTerm()
                << " to " << meta.term();
      partition->ConsensusUpdateTerm(meta.term());
    } else if (meta.term() < partition->ConsensusTerm()) /*outdated pb*/ {
      LOG(WARNING) << "Drop outdated trysync response " << table_name << ":" << partition_id
                   << " recv term: " << meta.term() << " local term: " << partition->ConsensusTerm();
      return;
    }

    if (response->consensus_meta().reject()) {
      Status s = TrySyncConsensusCheck(response->consensus_meta(), partition, slave_partition);
      if (!s.ok()) {
        slave_partition->SetReplState(ReplState::kError);
        LOG(WARNING) << "Consensus Check failed " << s.ToString();
      }
      return;
    }

    logic_last_offset = partition->ConsensusLastIndex().l_offset;
  }

  std::string partition_name = partition->PartitionName();
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    BinlogOffset boffset;
    int32_t session_id = try_sync_response.session_id();
    partition->Logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);
    slave_partition->SetMasterSessionId(session_id);
    LogOffset offset(boffset, logic_last_offset);
    g_pika_rm->SendPartitionBinlogSyncAckRequest(table_name, partition_id, offset, offset, true);
    slave_partition->SetReplState(ReplState::kConnected);
    // after connected, update receive time first to avoid connection timeout
    slave_partition->SetLastRecvTime(pstd::NowMicros());

    LOG(INFO) << "Partition: " << partition_name << " TrySync Ok";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    slave_partition->SetReplState(ReplState::kTryDBSync);
    LOG(INFO) << "Partition: " << partition_name << " Need To Try DBSync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    slave_partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error, Because the invalid filenum and offset";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    slave_partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error";
  }
}

Status PikaReplClientConn::TrySyncConsensusCheck(const InnerMessage::ConsensusMeta& consensus_meta,
                                                 const std::shared_ptr<SyncMasterPartition>& partition,
                                                 const std::shared_ptr<SyncSlavePartition>& slave_partition) {
  std::vector<LogOffset> hints;
  for (int i = 0; i < consensus_meta.hint_size(); ++i) {
    InnerMessage::BinlogOffset pb_offset = consensus_meta.hint(i);
    LogOffset offset;
    offset.b_offset.filenum = pb_offset.filenum();
    offset.b_offset.offset = pb_offset.offset();
    offset.l_offset.term = pb_offset.term();
    offset.l_offset.index = pb_offset.index();
    hints.push_back(offset);
  }
  LogOffset reply_offset;
  Status s = partition->ConsensusFollowerNegotiate(hints, &reply_offset);
  if (!s.ok()) {
    return s;
  }
  slave_partition->SetReplState(ReplState::kTryConnect);

  return s;
}

void PikaReplClientConn::DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse> res) {
  // partition to a bunch of binlog chips
  std::unordered_map<PartitionInfo, std::vector<int>*, hash_partition_info> par_binlog;
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);
    // hash key: table + partition_id
    PartitionInfo p_info(binlog_res.partition().table_name(), binlog_res.partition().partition_id());
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    par_binlog[p_info]->push_back(i);
  }

  std::shared_ptr<SyncSlavePartition> slave_partition = nullptr;
  for (auto& binlog_nums : par_binlog) {
    RmNode node(binlog_nums.first.table_name_, binlog_nums.first.partition_id_);
    slave_partition = g_pika_rm->GetSyncSlavePartitionByName(
        PartitionInfo(binlog_nums.first.table_name_, binlog_nums.first.partition_id_));
    if (!slave_partition) {
      LOG(WARNING) << "Slave Partition: " << binlog_nums.first.table_name_ << "_" << binlog_nums.first.partition_id_
                   << " not exist";
      break;
    }
    slave_partition->SetLastRecvTime(pstd::NowMicros());
    g_pika_rm->ScheduleWriteBinlogTask(binlog_nums.first.table_name_ + std::to_string(binlog_nums.first.partition_id_),
                                       res, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()),
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
