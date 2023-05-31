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
      auto task_arg =
          new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleTrySyncResponse, static_cast<void*>(task_arg));
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
  auto task_arg = static_cast<ReplClientTaskArg*>(arg);
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

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    const InnerMessage::InnerResponse_MetaSync_TableInfo& table_info = meta_sync.tables_info(idx);
    master_table_structs.push_back({table_info.table_name(), static_cast<uint32_t>(table_info.slot_num()), {0}});
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
  g_pika_server->PrepareSlotTrySync();
  g_pika_server->FinishMetaSync();
  LOG(INFO) << "Finish to handle meta sync response";
}

void PikaReplClientConn::HandleDBSyncResponse(void* arg) {
  std::unique_ptr<ReplClientTaskArg> task_arg(static_cast<ReplClientTaskArg*>(arg));
  std::shared_ptr<net::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  int32_t session_id = db_sync_response.session_id();
  const InnerMessage::Slot& slot_response = db_sync_response.slot();
  const std::string& table_name = slot_response.table_name();
  uint32_t slot_id = slot_response.slot_id();

  std::shared_ptr<SyncSlaveSlot> slave_slot =
      g_pika_rm->GetSyncSlaveSlotByName(SlotInfo(table_name, slot_id));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << table_name << ":" << slot_id << " Not Found";
    return;
  }

  if (response->code() != InnerMessage::kOk) {
    slave_slot->SetReplState(ReplState::kError);
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    return;
  }

  slave_slot->SetMasterSessionId(session_id);

  std::string slot_name = slave_slot->SlotName();
  slave_slot->SetReplState(ReplState::kWaitDBSync);
  LOG(INFO) << "Slot: " << slot_name << " Need Wait To Sync";
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
  const InnerMessage::Slot& slot_response = try_sync_response.slot();
  std::string table_name = slot_response.table_name();
  uint32_t slot_id = slot_response.slot_id();
  std::shared_ptr<SyncMasterSlot> slot =
      g_pika_rm->GetSyncMasterSlotByName(SlotInfo(table_name, slot_id));
  if (!slot) {
    LOG(WARNING) << "Partition: " << table_name << ":" << slot_id << " Not Found";
    return;
  }

  std::shared_ptr<SyncSlaveSlot> slave_slot =
      g_pika_rm->GetSyncSlaveSlotByName(SlotInfo(table_name, slot_id));
  if (!slave_slot) {
    LOG(WARNING) << "Slave Slot: " << table_name << ":" << slot_id << " Not Found";
    return;
  }

  LogicOffset logic_last_offset;
  if (response->has_consensus_meta()) {
    const InnerMessage::ConsensusMeta& meta = response->consensus_meta();
    if (meta.term() > slot->ConsensusTerm()) {
      LOG(INFO) << "Update " << table_name << ":" << slot_id << " term from " << slot->ConsensusTerm()
                << " to " << meta.term();
      slot->ConsensusUpdateTerm(meta.term());
    } else if (meta.term() < slot->ConsensusTerm()) /*outdated pb*/ {
      LOG(WARNING) << "Drop outdated trysync response " << table_name << ":" << slot_id
                   << " recv term: " << meta.term() << " local term: " << slot->ConsensusTerm();
      return;
    }

    if (response->consensus_meta().reject()) {
      Status s = TrySyncConsensusCheck(response->consensus_meta(), slot, slave_slot);
      if (!s.ok()) {
        slave_slot->SetReplState(ReplState::kError);
        LOG(WARNING) << "Consensus Check failed " << s.ToString();
      }
      return;
    }

    logic_last_offset = slot->ConsensusLastIndex().l_offset;
  }

  std::string slot_name = slot->SlotName();
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    BinlogOffset boffset;
    int32_t session_id = try_sync_response.session_id();
    slot->Logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);
    slave_slot->SetMasterSessionId(session_id);
    LogOffset offset(boffset, logic_last_offset);
    g_pika_rm->SendSlotBinlogSyncAckRequest(table_name, slot_id, offset, offset, true);
    slave_slot->SetReplState(ReplState::kConnected);
    // after connected, update receive time first to avoid connection timeout
    slave_slot->SetLastRecvTime(pstd::NowMicros());

    LOG(INFO) << "Slot: " << slot_name << " TrySync Ok";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    slave_slot->SetReplState(ReplState::kTryDBSync);
    LOG(INFO) << "Slot: " << slot_name << " Need To Try DBSync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    slave_slot->SetReplState(ReplState::kError);
    LOG(WARNING) << "Slot: " << slot_name << " TrySync Error, Because the invalid filenum and offset";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    slave_slot->SetReplState(ReplState::kError);
    LOG(WARNING) << "Slot: " << slot_name << " TrySync Error";
  }
}

Status PikaReplClientConn::TrySyncConsensusCheck(const InnerMessage::ConsensusMeta& consensus_meta,
                                                 const std::shared_ptr<SyncMasterSlot>& slot,
                                                 const std::shared_ptr<SyncSlaveSlot>& slave_slot) {
  std::vector<LogOffset> hints;
  for (int i = 0; i < consensus_meta.hint_size(); ++i) {
    const InnerMessage::BinlogOffset& pb_offset = consensus_meta.hint(i);
    LogOffset offset;
    offset.b_offset.filenum = pb_offset.filenum();
    offset.b_offset.offset = pb_offset.offset();
    offset.l_offset.term = pb_offset.term();
    offset.l_offset.index = pb_offset.index();
    hints.push_back(offset);
  }
  LogOffset reply_offset;
  Status s = slot->ConsensusFollowerNegotiate(hints, &reply_offset);
  if (!s.ok()) {
    return s;
  }
  slave_slot->SetReplState(ReplState::kTryConnect);

  return s;
}

void PikaReplClientConn::DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse>& res) {
  // partition to a bunch of binlog chips
  std::unordered_map<SlotInfo, std::vector<int>*, hash_partition_info> par_binlog;
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_res = res->binlog_sync(i);
    // hash key: table + slot_id
    SlotInfo p_info(binlog_res.slot().table_name(), binlog_res.slot().slot_id());
    if (par_binlog.find(p_info) == par_binlog.end()) {
      par_binlog[p_info] = new std::vector<int>();
    }
    par_binlog[p_info]->push_back(i);
  }

  std::shared_ptr<SyncSlaveSlot> slave_slot = nullptr;
  for (auto& binlog_nums : par_binlog) {
    RmNode node(binlog_nums.first.table_name_, binlog_nums.first.slot_id_);
    slave_slot = g_pika_rm->GetSyncSlaveSlotByName(
        SlotInfo(binlog_nums.first.table_name_, binlog_nums.first.slot_id_));
    if (!slave_slot) {
      LOG(WARNING) << "Slave Slot: " << binlog_nums.first.table_name_ << "_" << binlog_nums.first.slot_id_
                   << " not exist";
      break;
    }
    slave_slot->SetLastRecvTime(pstd::NowMicros());
    g_pika_rm->ScheduleWriteBinlogTask(binlog_nums.first.table_name_ + std::to_string(binlog_nums.first.slot_id_),
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
