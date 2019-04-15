// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include <sys/time.h>

#include "slash/include/slash_string.h"

#include "include/pika_rm.h"
#include "include/pika_server.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaReplicaManager* g_pika_rm;

PikaReplClientConn::PikaReplClientConn(int fd,
                               const std::string& ip_port,
                               pink::Thread* thread,
                               void* worker_specific_data,
                               pink::PinkEpoll* epoll)
      : pink::PbConn(fd, ip_port, thread, epoll) {
}

bool PikaReplClientConn::IsTableStructConsistent(
        const std::vector<TableStruct>& current_tables,
        const std::vector<TableStruct>& expect_tables) {
  if (current_tables.size() != expect_tables.size()) {
    return false;
  }
  for (const auto& table_struct : current_tables) {
    if (find(expect_tables.begin(), expect_tables.end(),
                table_struct) == expect_tables.end()) {
      return false;
    }
  }
  return true;
}

int PikaReplClientConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerResponse> response =  std::make_shared<InnerMessage::InnerResponse>();
  response->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  switch (response->type()) {
    case InnerMessage::kMetaSync:
    {
      ReplClientTaskArg* task_arg = new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleMetaSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kDBSync:
    {
      ReplClientTaskArg* task_arg = new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleDBSyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kTrySync:
    {
      ReplClientTaskArg* task_arg = new ReplClientTaskArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_rm->ScheduleReplClientBGTask(&PikaReplClientConn::HandleTrySyncResponse, static_cast<void*>(task_arg));
      break;
    }
    case InnerMessage::kBinlogSync:
    {
      DispatchBinlogRes(response);
      break;
    }
    default:
      break;
  }
  return 0;
}

void PikaReplClientConn::HandleMetaSyncResponse(void* arg) {
  ReplClientTaskArg* task_arg = static_cast<ReplClientTaskArg*>(arg);
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "Meta Sync Failed: " << reply;
    g_pika_server->SyncError();
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  const InnerMessage::InnerResponse_MetaSync meta_sync = response->meta_sync();
  if (g_pika_conf->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (g_pika_conf->classic_mode() ? "classic" : "sharding")
        << " mode, but master in " << (meta_sync.classic_mode() ? "classic" : "sharding")
        << " mode, failed to establish master-slave relationship";
    g_pika_server->SyncError();
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    InnerMessage::InnerResponse_MetaSync_TableInfo table_info = meta_sync.tables_info(idx);
    master_table_structs.emplace_back(table_info.table_name(), table_info.partition_num());
  }

  bool force_full_sync = g_pika_server->force_full_sync();
  std::vector<TableStruct> self_table_structs = g_pika_conf->table_structs();
  if (!force_full_sync
    && !PikaReplClientConn::IsTableStructConsistent(self_table_structs, master_table_structs)) {
    LOG(WARNING) << "Self table structs inconsistent with master"
        << ", failed to establish master-slave relationship";
    g_pika_server->SyncError();
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  if (force_full_sync) {
    LOG(INFO) << "Force full sync, need to rebuild table struct first";
    // Purge and rebuild Table Struct consistent with master
    if (!g_pika_server->RebuildTableStruct(master_table_structs)) {
      LOG(WARNING) << "Need force full sync but rebuild table struct error"
        << ", failed to establish master-slave relationship";
      g_pika_server->SyncError();
      conn->NotifyClose();
      delete task_arg;
      return;
    }
    g_pika_server->PurgeDir(g_pika_conf->trash_path());
  }

  g_pika_server->MetaSyncDone();
  g_pika_conf->SetWriteBinlog("yes");
  LOG(INFO) << "Finish to handle meta sync response";
  delete task_arg;
}

void PikaReplClientConn::HandleDBSyncResponse(void* arg) {
  ReplClientTaskArg* task_arg = static_cast<ReplClientTaskArg*>(arg);
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "DBSync Failed: " << reply;
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  const InnerMessage::InnerResponse_DBSync db_sync_response = response->db_sync();
  const InnerMessage::Partition partition_response = db_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id  = partition_response.partition_id();
  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id << " Not Found";
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  std::string partition_name = partition->GetPartitionName();
  if (db_sync_response.reply_code() == InnerMessage::InnerResponse::DBSync::kWait) {
    partition->SetReplState(ReplState::kWaitDBSync);
    LOG(INFO)    << "Partition: " << partition_name << " Need Wait To Sync";
  } else {
    partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "Partition: " << partition_name << " DBSync Error, Invaild Reply Code";
    conn->NotifyClose();
  }
  delete task_arg;
}

void PikaReplClientConn::HandleTrySyncResponse(void* arg) {
  ReplClientTaskArg* task_arg = static_cast<ReplClientTaskArg*>(arg);
  std::shared_ptr<pink::PbConn> conn = task_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> response = task_arg->res;

  if (response->code() != InnerMessage::kOk) {
    std::string reply = response->has_reply() ? response->reply() : "";
    LOG(WARNING) << "TrySync Failed: " << reply;
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  const InnerMessage::InnerResponse_TrySync try_sync_response = response->try_sync();
  const InnerMessage::Partition partition_response = try_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id  = partition_response.partition_id();
  std::shared_ptr<Partition> partition = g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    LOG(WARNING) << "Partition: " << table_name << ":" << partition_id << " Not Found";
    conn->NotifyClose();
    delete task_arg;
    return;
  }

  std::string partition_name = partition->GetPartitionName();
  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    BinlogOffset boffset;
    partition->SetReplState(ReplState::kConnected);
    partition->logger()->GetProducerStatus(&boffset.filenum, &boffset.offset);
    g_pika_server->SendPartitionBinlogSyncAckRequest(table_name, partition_id, boffset, boffset, true);
    LOG(INFO)    << "Partition: " << partition_name << " TrySync Ok";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointBePurged) {
    partition->SetReplState(ReplState::kTryDBSync);
    LOG(INFO)    << "Partition: " << partition_name << " Need To Try DBSync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kSyncPointLarger) {
    partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error, Because the invalid filenum and offset";
    conn->NotifyClose();
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    partition->SetReplState(ReplState::kError);
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error";
    conn->NotifyClose();
  }
  delete task_arg;
}

void PikaReplClientConn::DispatchBinlogRes(const std::shared_ptr<InnerMessage::InnerResponse> res) {
  // partition to a bunch of binlog chips
  std::unordered_map<std::string, std::vector<int>*> par_binlog;
  for (int i = 0; i < res->binlog_sync_size(); ++i) {
    const InnerMessage::InnerResponse::BinlogSync& binlog_response = res->binlog_sync(i);
    // hash key: table + partition_id
    InnerMessage::Partition partition = binlog_response.partition();
    std::string key = partition.table_name() + std::to_string(partition.partition_id());
    if (par_binlog.find(key) == par_binlog.end()) {
      par_binlog[key] = new std::vector<int>();
    }
    par_binlog[key]->push_back(i);
  }

  for (auto& binlog_nums : par_binlog) {
    g_pika_rm->ScheduleWriteBinlogTask(
        binlog_nums.first,
        res,
        std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()),
        reinterpret_cast<void*>(binlog_nums.second));
  }
}

