// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"

#include "include/pika_server.h"
#include "src/pika_inner_message.pb.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;

PikaReplClientConn::PikaReplClientConn(int fd,
                               const std::string& ip_port,
                               pink::Thread* thread,
                               void* worker_specific_data,
                               pink::PinkEpoll* epoll)
      : pink::PbConn(fd, ip_port, thread, epoll) {
}

void PikaReplClientConn::DoReplClientTask(void* arg) {
  InnerMessage::InnerResponse* response = reinterpret_cast<InnerMessage::InnerResponse*>(arg);
  delete response;
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

int PikaReplClientConn::HandleMetaSyncResponse(const InnerMessage::InnerResponse& response) {
  const InnerMessage::InnerResponse_MetaSync meta_sync = response.meta_sync();
  if (g_pika_conf->classic_mode() != meta_sync.classic_mode()) {
    LOG(WARNING) << "Self in " << (g_pika_conf->classic_mode() ? "classic" : "sharding")
        << " mode, but master in " << (meta_sync.classic_mode() ? "classic" : "sharding")
        << " mode, failed to establish a master-slave relationship";
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
        << " ,failed to establish a master-slave relationship";
    g_pika_server->SyncError();
    return -1;
  }

  if (force_full_sync) {
    // Purge and rbuild Table Struct consistent with master
    g_pika_server->RebuildTableStruct(master_table_structs);
    g_pika_server->PurgeDir(g_pika_conf->trash_path());
  }

  g_pika_server->MetaSyncDone();
  return 0;
}

int PikaReplClientConn::DealMessage() {
  int res = 0;
  InnerMessage::InnerResponse response;
  response.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  switch (response.type()) {
    case InnerMessage::kMetaSync:
      res = HandleMetaSyncResponse(response);
      break;
    default:
      break;
  }
  return res;
}
