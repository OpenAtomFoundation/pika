// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_server_conn.h"

#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

PikaReplServerConn::PikaReplServerConn(int fd,
                                       std::string ip_port,
                                       pink::Thread* thread,
                                       void* worker_specific_data)
    : PbConn(fd, ip_port, thread),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<PikaBinlogReceiverThread*>(worker_specific_data);
  pink::RedisParserSettings settings;
  settings.DealMessage = &(PikaReplServerConn::ParserDealMessage);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

PikaReplServerConn::~PikaReplServerConn() {
}

int PikaReplServerConn::DealMessage() {
  InnerMessage::InnerRequest req;
  req.ParseFromArray(rbuf_ + cur_pos_ - header_len_, header_len_);
  int res = 0;
  switch (req.type()) {
    case InnerMessage::kMetaSync:
      break;
    case InnerMessage::kTrySync:
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

int PikaReplServerConn::HandleBinlogSync(const InnerMessage::InnerRequest& req) {
  for (int i = 0; i < req.binlog_sync_size(); ++i) {
    const InnerMessage::InnerRequest::BinlogSync& binlog_req = req.binlog_sync(i);
    int processed_len = 0;
    int scrubed_len = 0;
    pink::ReadStatus scrub_status = binlog_parser_.ScrubReadBuffer(
        binlog_req.binlog().data(),
        binlog_req.binlog().size(),
        &processed_len,
        &scrubed_len,
        &binlog_header_,
        &binlog_item_);
    if (scrub_status != pink::kReadAll
        || processed_len != static_cast<int>(binlog_req.binlog().size())) {
      return -1;
    }
    const char* redis_parser_start = binlog_req.binlog().data() + scrubed_len;
    int redis_parser_len = static_cast<int>(binlog_req.binlog().size()) - scrubed_len;
    pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
      redis_parser_start, redis_parser_len, &processed_len);
    if (ret != pink::kRedisParserDone) {
      return -1;
    }
  }
  return 0;
}

bool PikaReplServerConn::ProcessAuth(const pink::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }
  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_server->sid())) {
      is_authed_ = true;
      g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
      return true;
    }
  }
  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
  return false;
}

bool PikaReplServerConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item) {
  if (!is_authed_) {
    LOG(INFO) << "Need Auth First";
    return false;
  } else if (argv.empty()) {
    return false;
  } else {
    g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);
  }

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
  PikaBinlogReceiverConn* conn = reinterpret_cast<PikaBinlogReceiverConn*>(parser->data);
  if (conn->binlog_header_.header_type_ == kTypeAuth) {
    return conn->ProcessAuth(argv) == true ? 0 : -1;
  } else if (conn->binlog_header_.header_type_ == kTypeBinlog) {
    return conn->ProcessBinlogData(argv, conn->binlog_item_) == true ? 0 : -1;
  }
  return -1;
}
