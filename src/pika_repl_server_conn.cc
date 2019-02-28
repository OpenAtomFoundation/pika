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

int PikaReplServerConn::HandleTrySync(const InnerMessage::InnerRequest& req) {
  InnerMessage::InnerRequest::TrySync try_sync_request = req.try_sync();
  InnerMessage::Partition partition_request = try_sync_request.partition();
  std::string table_name = partition_request.table_name();
  uint32_t partition_id = partition_request.partition_id();
  bool force = try_sync_request.force();
  std::string partition_name = table_name + "_" + std::to_string(partition_id);
  InnerMessage::BinlogOffset slave_boffset = try_sync_request.binlog_offset();
  InnerMessage::Node node = try_sync_request.node();
  LOG(INFO) << "Trysync, Slave ip: " << node.ip() << ", Slave port:"
    << node.port() << ", Partition: " << partition_name << ", filenum: "
    << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset()
    << ", force: " << (force ? "yes" : "no");

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::Type::kTrySync);
  response.set_code(InnerMessage::StatusCode::kOk);
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);
  if (force) {
    LOG(INFO) << "Partition: " << partition_name << " force full sync, BgSave and DbSync first";
    g_pika_server->TryDBSync(node.ip(), node.port(), table_name, partition_id, slave_boffset.filenum());
    try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kWait);
  } else {
    BinlogOffset boffset;
    if (!g_pika_server->GetTablePartitionBinlogOffset(table_name, partition_id, &boffset)) {
      try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kError);
      LOG(WARNING) << "Handle TrySync, Partition: "
        << partition_name << " not found, TrySync failed";
    } else {
      if (boffset.filenum < slave_boffset.filenum()
        || (boffset.filenum == slave_boffset.filenum() && boffset.offset < slave_boffset.offset())) {
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kInvalidOffset);
        LOG(WARNING) << "Slave offset is larger than mine, Slave ip: "
          << node.ip() << ", Slave port: " << node.port() << ", Partition: "
          << partition_name << ", filenum: " << slave_boffset.filenum()
          << ", pro_offset_: " << slave_boffset.offset() << ", force: "
          << (force ? "yes" : "no");
      } else {
        LOG(INFO) << "Partition: " << partition_name << " TrySync success";
        try_sync_response->set_reply_code(InnerMessage::InnerResponse::TrySync::kOk);
        try_sync_response->set_sid(0);
      }
    }
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || WriteResp(reply_str)) {
    return -1;
  }
  NotifyWrite();
  return 0;
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
