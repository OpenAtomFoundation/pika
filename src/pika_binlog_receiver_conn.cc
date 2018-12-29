// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include "include/pika_binlog_receiver_conn.h"

#include <glog/logging.h>

#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverConn::PikaBinlogReceiverConn(int fd,
                                               std::string ip_port,
                                               void* worker_specific_data)
    : PinkConn(fd, ip_port, NULL),
      rbuf_(nullptr),
      rbuf_len_(0),
      msg_peak_(0),
      is_authed_(false),
      last_read_pos_(-1),
      bulk_len_(-1) {
  binlog_receiver_ = reinterpret_cast<PikaBinlogReceiverThread*>(worker_specific_data);
  pink::RedisParserSettings settings;
  settings.DealMessage = &(PikaBinlogReceiverConn::DealMessage);
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;  
}

PikaBinlogReceiverConn::~PikaBinlogReceiverConn() {
  free(rbuf_);
}

pink::ReadStatus PikaBinlogReceiverConn::ParseRedisParserStatus(pink::RedisParserStatus status) {
  if (status == pink::kRedisParserInitDone) {
    return pink::kOk;
  } else if (status == pink::kRedisParserHalf) {
    return pink::kReadHalf;
  } else if (status == pink::kRedisParserDone) {
    return pink::kReadAll;
  } else if (status == pink::kRedisParserError) {
    pink::RedisParserError error_code = redis_parser_.get_error_code();
    switch (error_code) {
      case pink::kRedisParserOk :
        // status is error cant be ok
        return pink::kReadError;
      case pink::kRedisParserInitError :
        return pink::kReadError;
      case pink::kRedisParserFullError :
        return pink::kFullError;
      case pink::kRedisParserProtoError :
        return pink::kParseError;
      case pink::kRedisParserDealError :
        return pink::kDealError;
      default :
        return pink::kReadError;
    }
  } else {
    return pink::kReadError;
  }
}

pink::ReadStatus PikaBinlogReceiverConn::GetRequest() {
  ssize_t nread = 0;
  int next_read_pos = last_read_pos_ + 1;

  int remain = rbuf_len_ - next_read_pos;  // Remain buffer size
  int new_size = 0;
  if (remain == 0) {
    new_size = rbuf_len_ + REDIS_IOBUF_LEN;
    remain += REDIS_IOBUF_LEN;
  } else if (remain < bulk_len_) {
    new_size = next_read_pos + bulk_len_;
    remain = bulk_len_;
  }
  if (new_size > rbuf_len_) {
    if (new_size > REDIS_MAX_MESSAGE) {
      return pink::kFullError;
    }
    rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
    if (rbuf_ == nullptr) {
      return pink::kFullError;
    }
    rbuf_len_ = new_size;
  }

  nread = read(fd(), rbuf_ + next_read_pos, remain);
  if (nread == -1) {
    if (errno == EAGAIN) {
      nread = 0;
      return pink::kReadHalf; // HALF
    } else {
      // error happened, close client
      return pink::kReadError;
    }
  } else if (nread == 0) {
    // client closed, close client
    return pink::kReadClose;
  }

  // assert(nread > 0);
  last_read_pos_ += nread;
  msg_peak_ = last_read_pos_;

  char* cur_process_p = rbuf_ + next_read_pos;
  int processed_len = 0;
  pink::ReadStatus read_status = pink::kReadError;
  while (processed_len < nread) {
    int remain = nread - processed_len;
    int scrubed_len = 0; // len binlog parser may scrub 
    char* cur_scrub_start = cur_process_p + processed_len;
    int cur_bp_processed_len = 0; // current binlog parser processed len
    pink::ReadStatus scrub_status = binlog_parser_.ScrubReadBuffer(cur_scrub_start, remain, &cur_bp_processed_len, &scrubed_len, &binlog_header_, &binlog_item_);
    processed_len += cur_bp_processed_len;
    // return kReadAll or kReadHalf or kReadError
    if (scrub_status != pink::kReadAll) {
      read_status = scrub_status;
      break;
    }

    char* cur_redis_parser_start = cur_scrub_start + scrubed_len;
    int cur_parser_buff_len = cur_bp_processed_len - scrubed_len;
    int redis_parser_processed_len = 0; // parsed len already updated by cur_bp_processed_len useless here

    pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
      cur_redis_parser_start, cur_parser_buff_len, &redis_parser_processed_len);
    read_status = ParseRedisParserStatus(ret);
    if (read_status != pink::kReadAll) {
      break; 
    }
  }
  if (read_status == pink::kReadAll || read_status == pink::kReadHalf) {
    last_read_pos_ = -1;
    bulk_len_ = redis_parser_.get_bulk_len();
  }
  return read_status;
}


pink::WriteStatus PikaBinlogReceiverConn::SendReply() {
  return pink::kWriteAll;
}

bool PikaBinlogReceiverConn::ProcessAuth(const pink::RedisCmdArgsType& argv) {
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

bool PikaBinlogReceiverConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item) {
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
    Cmd* c_ptr = binlog_receiver_->GetCmd(slash::StringToLower(opt));
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

int PikaBinlogReceiverConn::DealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
  PikaBinlogReceiverConn* conn = reinterpret_cast<PikaBinlogReceiverConn*>(parser->data);
  if (conn->binlog_header_.header_type_ == kTypeAuth) {
    return conn->ProcessAuth(argv) == true ? 0 : -1;
  } else if (conn->binlog_header_.header_type_ == kTypeBinlog) {
    return conn->ProcessBinlogData(argv, conn->binlog_item_) == true ? 0 : -1;
  }
  return -1;
}
