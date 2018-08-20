// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"
#include <glog/logging.h>
#include "include/pika_new_master_conn.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_binlog_transverter.h"
#include "include/pika_binlog_receiver_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaNewMasterConn::PikaNewMasterConn(int fd,
                                     std::string ip_port,
                                     void* worker_specific_data)
    : PinkConn(fd, ip_port, NULL),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_size_(REDIS_IOBUF_LEN),
      rbuf_cur_pos_(0),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<PikaBinlogReceiverThread*>(worker_specific_data);
  rbuf_ = static_cast<char*>(realloc(rbuf_, REDIS_IOBUF_LEN));
}

PikaNewMasterConn::~PikaNewMasterConn() {
  free(rbuf_);
}

pink::ReadStatus PikaNewMasterConn::ReadRaw(uint32_t count) {
  if (rbuf_cur_pos_ + count > rbuf_size_) {
    return pink::kFullError;
  }
  int32_t nread = read(fd(), rbuf_ + rbuf_len_, count - (rbuf_len_ - rbuf_cur_pos_));
  if (nread == -1) {
    if (errno == EAGAIN) {
      return pink::kReadHalf;
    } else {
      return pink::kReadError;
    }
  } else if (nread == 0) {
    return pink::kReadClose;
  }

  rbuf_len_ += nread;
  if (rbuf_len_ - rbuf_cur_pos_ != count) {
    return pink::kReadHalf;
  }
  return pink::kReadAll;
}

pink::ReadStatus PikaNewMasterConn::ReadHeader() {
  if (rbuf_len_ >= HEADER_LEN) {
    return pink::kReadAll;
  }

  pink::ReadStatus status = ReadRaw(HEADER_LEN);
  if (status != pink::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += HEADER_LEN;
  return pink::kReadAll;
}

pink::ReadStatus PikaNewMasterConn::ReadBody(uint32_t body_length) {
  if (rbuf_len_ == HEADER_LEN + body_length) {
    return pink::kReadAll;
  } else if (rbuf_len_ > HEADER_LEN + body_length) {
    LOG(INFO) << "rbuf_len_ larger than sum of header length (6 Byte) and body_length, rbuf_len_:"
      << rbuf_len_ << " body_length:" << body_length;
  }

  pink::ReadStatus status = ReadRaw(body_length);
  if (status != pink::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += body_length;
  return pink::kReadAll;
}

int32_t PikaNewMasterConn::FindNextSeparators(const std::string& content, int32_t pos) {
  int32_t length = content.size();
  if (pos >= length) {
    return -1;
  }
  while (pos < length) {
    if (content[pos] == '\n') {
      return pos;
    }
    pos++;
  }
  return -1;
}

int32_t PikaNewMasterConn::GetNextNum(const std::string& content, int32_t left_pos, int32_t right_pos, long* value) {
  //  left_pos        right_pos
  //      |------   -------|
  //            |   |
  //            *3\r\n
  //            012 3
  // num range [left_pos + 1, right_pos - 2]
  assert(left_pos < right_pos);
  if (slash::string2l(content.data() + left_pos + 1,  right_pos - left_pos - 2, value)) {
    return 0;
  }
  return -1;
}

// RedisRESPArray : *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
pink::ReadStatus PikaNewMasterConn::ParseRedisRESPArray(const std::string& content, pink::RedisCmdArgsType* argv) {

  int32_t pos = 0;
  int32_t next_parse_pos = 0;
  int32_t content_len = content.size();
  long multibulk_len = 0, bulk_len = 0;
  if (content.empty() || content[0] != '*') {
    LOG(INFO) << "Content empty() or the first character of the redis protocol string not equal '*'";
    return pink::kParseError;
  }
  pos = FindNextSeparators(content, next_parse_pos);
  if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &multibulk_len) != -1) {
    next_parse_pos = pos + 1;
  } else {
    LOG(INFO) << "Find next separators error or get next num error";
    return pink::kParseError;
  }

  // next_parst_pos          pos
  //        |-------   -------|
  //               |   |
  //               $3\r\nset\r\n
  //               012 3 4567 8

  argv->clear();
  while (multibulk_len) {
    if (content[next_parse_pos] != '$') {
      LOG(INFO) << "The first charactor of the RESP type element not equal '$'";
      return pink::kParseError;
    }

    bulk_len = -1;
    pos = FindNextSeparators(content, next_parse_pos);
    if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &bulk_len) != -1) {
      if (pos + 1 + bulk_len + 2 > content_len) {
        return pink::kParseError;
      } else {
        next_parse_pos = pos + 1;
        argv->emplace_back(content.data() + next_parse_pos, bulk_len);
        next_parse_pos = next_parse_pos + bulk_len + 2;
        multibulk_len--;
      }
    } else {
      LOG(INFO) << "Find next separators error or get next num error";
      return pink::kParseError;
    }
  }
  if (content_len != next_parse_pos) {
    LOG(INFO) << "Incomplete parse";
    return pink::kParseError;
  } else {
    return pink::kOk;
  }
}

void PikaNewMasterConn::ResetStatus() {
  rbuf_len_ = 0;
  rbuf_cur_pos_ = 0;
}

pink::ReadStatus PikaNewMasterConn::GetRequest() {
  // Read Header
  pink::ReadStatus status;
  if ((status = ReadHeader()) != pink::kReadAll) {
    return status;
  }

  // Process Header, get body length;
  uint16_t type = 0;
  uint32_t body_length = 0;
  std::string header(rbuf_, HEADER_LEN);
  slash::GetFixed16(&header, &type);
  slash::GetFixed32(&header, &body_length);

  if (type != kTypeAuth && type != kTypeBinlog) {
    LOG(INFO) << "Unrecognizable Type: " << type << " maybe identify binlog type error";
    return pink::kParseError;
  }

  // Realloc buffer according header_len and body_length if need
  uint32_t needed_size = HEADER_LEN + body_length;
  if (rbuf_size_ < needed_size) {
    if (needed_size > REDIS_MAX_MESSAGE) {
      return pink::kFullError;
    } else {
      rbuf_ = static_cast<char*>(realloc(rbuf_, needed_size));
      rbuf_size_ = needed_size;
    }
  }

  // Read Body
  if ((status = ReadBody(body_length)) != pink::kReadAll) {
    return status;
  }

  pink::RedisCmdArgsType argv;
  std::string body(rbuf_ + HEADER_LEN, body_length);
  if (type == kTypeAuth) {
    if ((status = ParseRedisRESPArray(body, &argv)) != pink::kOk) {
      LOG(INFO) << "Type auth ParseRedisRESPArray error";
      return status;
    }
    if (!ProcessAuth(argv)) {
      return pink::kDealError;
    }
  } else if (type == kTypeBinlog) {
    BinlogItem item;
    if (!PikaBinlogTransverter::BinlogDecode(TypeFirst, body, &item)) {
      LOG(INFO) << "Binlog decode error: " << item.ToString().c_str();
      return pink::kParseError;
    }
    if ((status = ParseRedisRESPArray(item.content(), &argv)) != pink::kOk) {
      LOG(INFO) << "Type Binlog ParseRedisRESPArray error: " << item.ToString().c_str();
      return status;
    }
    if (!ProcessBinlogData(argv, item)) {
      return pink::kDealError;
    }
  } else {
    LOG(INFO) << "Unrecognizable Type";
    return pink::kParseError;
  }

  // Reset status
  ResetStatus();
  return pink::kReadAll;
}

pink::WriteStatus PikaNewMasterConn::SendReply() {
  return pink::kWriteAll;
}

void PikaNewMasterConn::TryResizeBuffer() {
}

bool PikaNewMasterConn::ProcessAuth(const pink::RedisCmdArgsType& argv) {
  g_pika_server->PlusThreadQuerynum();
  if (argv.empty() || argv.size() != 2) {
    return false;
  }
  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_server->sid())) {
      is_authed_ = true;
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
      return true;
    }
  }
  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_server->sid() << " Master auth server id: " << argv[1];
  return false;
}

bool PikaNewMasterConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item) {
  if (!is_authed_) {
    LOG(INFO) << "Need Auth First";
    return false;
  } else if (argv.empty()) {
    return false;
  } else {
    g_pika_server->PlusThreadQuerynum();
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

  bool is_readonly = g_pika_conf->readonly();

  // Here, the binlog dispatch thread, instead of the binlog bgthread takes on the task to write binlog
  // Only when the server is readonly
  uint64_t serial = binlog_receiver_->GetnPlusSerial();
  if (is_readonly) {
    if (!g_pika_server->WaitTillBinlogBGSerial(serial)) {
      return false;
    }
    std::string opt = argv[0];
    Cmd* c_ptr = binlog_receiver_->GetCmd(slash::StringToLower(opt));

    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(c_ptr->ToBinlog(argv,
                                                binlog_item.exec_time(),
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

