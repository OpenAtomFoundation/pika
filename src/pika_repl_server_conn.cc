// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       std::string ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
}

PikaReplServerConn::~PikaReplServerConn() {
}

int PikaReplServerConn::DealMessage() {
  std::shared_ptr<InnerMessage::InnerRequest> req = std::make_shared<InnerMessage::InnerRequest>();
  bool parse_res = req->ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  int res = 0;
  switch (req->type()) {
    case InnerMessage::kMetaSync:
      g_pika_server->ScheduleReplMetaSyncTask(
          req,
          std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()),
          NULL);
      break;
    case InnerMessage::kTrySync:
      g_pika_server->ScheduleReplTrySyncTask(
          req,
          std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()),
          NULL);
      break;
    case InnerMessage::kBinlogSync:
      DispatchBinlogReq(req);
      break;
    default:
      break;
  }
  return res;
}

void PikaReplServerConn::DispatchBinlogReq(const std::shared_ptr<InnerMessage::InnerRequest> req) {
  // partition to a bunch of binlog chips
  std::unordered_map<std::string, std::vector<int>*> par_binlog;
  for (int i = 0; i < req->binlog_sync_size(); ++i) {
    const InnerMessage::InnerRequest::BinlogSync& binlog_req = req->binlog_sync(i);
    // hash key: table + partition_id
    std::string key = binlog_req.table_name() + std::to_string(binlog_req.partition_id());
    if (par_binlog.find(key) == par_binlog.end()) {
      par_binlog[key] = new std::vector<int>();
    }
    par_binlog[key]->push_back(i);
  }
  for (auto& binlog_nums : par_binlog) {
    g_pika_server->ScheduleReplBinlogSyncTask(
        binlog_nums.first,
        req,
        std::dynamic_pointer_cast<PikaReplServerConn>(shared_from_this()),
        reinterpret_cast<void*>(binlog_nums.second));
  }
}
