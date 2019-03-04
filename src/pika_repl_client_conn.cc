// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include <sys/time.h>

#include "include/pika_server.h"
#include "slash/include/slash_string.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;

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

int PikaReplClientConn::HandleMetaSyncResponse(const std::shared_ptr<InnerMessage::InnerResponse>  response) {
  const InnerMessage::InnerResponse_MetaSync meta_sync = response->meta_sync();
  if (g_pika_conf->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (g_pika_conf->classic_mode() ? "classic" : "sharding")
        << " mode, but master in " << (meta_sync.classic_mode() ? "classic" : "sharding")
        << " mode, failed to establish master-slave relationship";
    g_pika_server->SyncError();
    return -1;
  }

  std::vector<TableStruct> master_table_structs;
  for (int idx = 0; idx < meta_sync.tables_info_size(); ++idx) {
    InnerMessage::InnerResponse_MetaSync_TableInfo table_info = meta_sync.tables_info(idx);
    master_table_structs.emplace_back(table_info.table_name(), table_info.partition_num());
  }

  bool force_full_sync = g_pika_server->force_full_sync();
  std::vector<TableStruct> self_table_structs = g_pika_conf->table_structs();
  if (!force_full_sync
    && !IsTableStructConsistent(self_table_structs, master_table_structs)) {
    LOG(WARNING) << "Self table structs inconsistent with master"
        << ", failed to establish master-slave relationship";
    g_pika_server->SyncError();
    return -1;
  }

  if (force_full_sync) {
    LOG(INFO) << "Force full sync, need to rebuild table struct first";
    // Purge and rebuild Table Struct consistent with master
    if (!g_pika_server->RebuildTableStruct(master_table_structs)) {
      LOG(WARNING) << "Need force full sync but rebuild table struct error"
        << ", failed to establish master-slave relationship";
      g_pika_server->SyncError();
      return -1;
    }
    g_pika_server->PurgeDir(g_pika_conf->trash_path());
  }
  LOG(INFO) << "Finish to handle meta sync response";
  g_pika_server->MetaSyncDone();
  return 0;
}

int PikaReplClientConn::HandleTrySyncResponse(const InnerMessage::InnerResponse& response) {
  const InnerMessage::InnerResponse_TrySync try_sync_response = response.try_sync();
  const InnerMessage::Partition partition_response = try_sync_response.partition();
  std::string table_name = partition_response.table_name();
  uint32_t partition_id  = partition_response.partition_id();
  std::string partition_name = table_name + "_" + std::to_string(partition_id);

  if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kError) {
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kWait) {
    LOG(WARNING) << "Partition: " << partition_name << " Need wait to sync";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kInvalidOffset) {
    LOG(WARNING) << "Partition: " << partition_name << " TrySync Error, Because the invalid filenum and offset";
  } else if (try_sync_response.reply_code() == InnerMessage::InnerResponse::TrySync::kOk) {
    LOG(INFO)    << "Partition: " << partition_name << " TrySync Ok";
  }

  int PikaReplClientConn::HandleBinlogSyncResponse(const InnerMessage::InnerResponse& response) {
  return 0;
}

int PikaReplClientConn::DealMessage() {
  int res = 0;
  std::shared_ptr<InnerMessage::InnerResponse> response =  std::make_shared<InnerMessage::InnerResponse>();
  response->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  switch (response->type()) {
    case InnerMessage::kMetaSync:
      res = HandleMetaSyncResponse(response);
      break;
    case InnerMessage::kTrySync:
      res = HandleTrySyncResponse(response);
      break;
    case InnerMessage::kBinlogSync:
    {
      ReplRespArg* arg = new ReplRespArg(response, std::dynamic_pointer_cast<PikaReplClientConn>(shared_from_this()));
      g_pika_server->ScheduleReplCliTask(&PikaReplClientConn::HandleBinlogSyncResponse, static_cast<void*>(arg));
      res = 0;
    }
    default:
      break;
  }
  return res;
}

void PikaReplClientConn::HandleBinlogSyncResponse(void* arg) {
  ReplRespArg* resp_arg = static_cast<ReplRespArg*>(arg);
  std::shared_ptr<pink::PbConn> conn = resp_arg->conn;
  std::shared_ptr<InnerMessage::InnerResponse> resp = resp_arg->resp;
  if (!resp->has_binlog_sync()) {
    LOG(WARNING) << "Pb parse error";
    delete resp_arg;
    return;
  }
  const InnerMessage::InnerResponse_BinlogSync& binlog_ack = resp->binlog_sync();
  std::string table_name = binlog_ack.table_name();
  uint32_t partition_id = binlog_ack.partition_id();
  std::string ip;
  int port = 0;
  bool res = slash::ParseIpPortString(conn->ip_port(), ip, port);
  if (!res) {
    LOG(WARNING) << "Parse Error ParseIpPortString faile";
    delete resp_arg;
    return;
  }
  const InnerMessage::SyncOffset& sync_offset = binlog_ack.sync_offset();

  uint64_t now;
  struct timeval tv;
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;

  // Set ack info from slave
  res = g_pika_server->SetBinlogAckInfo(table_name, partition_id, ip, port, sync_offset.filenum(), sync_offset.offset(), now);
  if (!res) {
    LOG(WARNING) << "Update binlog ack failed " << table_name << " " << partition_id;
    delete resp_arg;
    return;
  }
  delete resp_arg;

  Status s = g_pika_server->SendBinlogSyncRequest(table_name, partition_id, ip, port);
  if (!s.ok()) {
    LOG(WARNING) << "Send BinlogSync Request failed " << table_name << " " << partition_id;
    return;
  }
}
