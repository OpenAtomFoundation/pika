// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include <glog/logging.h>

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       std::string ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data, pink::PinkEpoll* epoll)
    : PbConn(fd, ip_port, thread, epoll) {
  binlog_receiver_ = reinterpret_cast<PikaReplServerThread*>(worker_specific_data);
  pink::RedisParserSettings settings;
  settings.DealMessage = &(PikaReplServerConn::ParserDealMessage);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

PikaReplServerConn::~PikaReplServerConn() {
}

int PikaReplServerConn::DealMessage() {
  InnerMessage::InnerRequest req;
  bool parse_res = req.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  if (!parse_res) {
    LOG(WARNING) << "Pika repl server connection pb parse error.";
    return -1;
  }
  int res = 0;
  switch (req.type()) {
    case InnerMessage::kMetaSync:
      HandleMetaSyncRequest(req);
      break;
    case InnerMessage::kTrySync:
      HandleTrySync(req);
      break;
    case InnerMessage::kBinlogSync:
      res = HandleBinlogSync(req);
    case InnerMessage::kDbSync:
      break;
    default:
      break;
  }
  return res;
}

int PikaReplServerConn::HandleMetaSyncRequest(const InnerMessage::InnerRequest& req) {
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  InnerMessage::InnerResponse response;
  response.set_code(InnerMessage::StatusCode::kOk);
  response.set_type(InnerMessage::Type::kMetaSync);
  InnerMessage::InnerResponse_MetaSync* meta_sync = response.mutable_meta_sync();
  meta_sync->set_classic_mode(g_pika_conf->classic_mode());
  for (const auto& table_struct : table_structs) {
    InnerMessage::InnerResponse_MetaSync_TableInfo* table_info = meta_sync->add_tables_info();
    table_info->set_table_name(table_struct.table_name);
    table_info->set_partition_num(table_struct.partition_num);
  }

  std::string reply_str;
  if (!response.SerializeToString(&reply_str)
    || WriteResp(reply_str)) {
    return -1;
  }
  NotifyWrite();
  return 0;
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
    << slave_boffset.filenum() << ", pro_offset: " << slave_boffset.offset();

  InnerMessage::InnerResponse response;
  response.set_type(InnerMessage::Type::kTrySync);
  response.set_code(InnerMessage::StatusCode::kOk);
  InnerMessage::InnerResponse::TrySync* try_sync_response = response.mutable_try_sync();
  InnerMessage::Partition* partition_response = try_sync_response->mutable_partition();
  partition_response->set_table_name(table_name);
  partition_response->set_partition_id(partition_id);
  if (force) {
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
          << ", pro_offset_: " <<  slave_boffset.offset();
      } else {
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

int PikaReplServerConn::HandleBinlogSync(const InnerMessage::InnerRequest& req) {
  for (int i = 0; i < req.binlog_sync_size(); ++i) {
    const InnerMessage::InnerRequest::BinlogSync& binlog_req = req.binlog_sync(i);
    if(!PikaBinlogTransverter::BinlogItemWithoutContentDecode(TypeFirst, binlog_req.binlog(), &binlog_item_)) {
      return -1;
    }
    const char* redis_parser_start = binlog_req.binlog().data() + BINLOG_ENCODE_LEN;
    int redis_parser_len = static_cast<int>(binlog_req.binlog().size()) - BINLOG_ENCODE_LEN;
    int processed_len = 0;
    pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
      redis_parser_start, redis_parser_len, &processed_len);
    if (ret != pink::kRedisParserDone) {
      return -1;
    }
  }
  return 0;
}


bool PikaReplServerConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item) {
  g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);

  // Monitor related
  std::string monitor_message;
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0 * slash::NowMicros() / 1000000)
      + " [" + this->ip_port() + "]";
    for (const auto& item : argv) {
      monitor_message += " " + slash::ToRead(item);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  bool is_readonly = g_pika_server->readonly();

  // Here, the binlog dispatch thread, instead of the binlog bgthread takes on the task to write binlog
  // Only when the server is readonly
  uint64_t serial = binlog_receiver_->GetnPlusSerial();
  if (is_readonly) {
    if (!g_pika_server->WaitTillBinlogBGSerial(serial)) {
      return false;
    }
    std::string opt = argv[0];
    Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(slash::StringToLower(opt));
    c_ptr->Initial(argv, g_pika_conf->default_table());

    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(c_ptr->ToBinlog(binlog_item.exec_time(),
                                                std::to_string(binlog_item.server_id()),
                                                binlog_item.logic_id(),
                                                binlog_item.filenum(),
                                                binlog_item.offset()));
    g_pika_server->logger_->Unlock();
    g_pika_server->SignalNextBinlogBGSerial();
  }

  PikaCmdArgsType *v = new PikaCmdArgsType(argv);
  BinlogItem *b = new BinlogItem(binlog_item);
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, v, b, serial, is_readonly);
  return true;
}

int PikaReplServerConn::ParserDealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  PikaReplServerConn* conn = reinterpret_cast<PikaReplServerConn*>(parser->data);
  return conn->ProcessBinlogData(argv, conn->binlog_item_) == true ? 0 : -1;
}
