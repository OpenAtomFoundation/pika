// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"

#include "binlog_transverter.h"
#include "const.h"
#include "master_conn.h"
#include "binlog_receiver_thread.h"
#include "pika_port.h"

extern PikaPort *g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port, void* worker_specific_data)
    : PinkConn(fd, ip_port, NULL),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_size_(REDIS_IOBUF_LEN),
      rbuf_cur_pos_(0),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<BinlogReceiverThread*>(worker_specific_data);
  rbuf_ = static_cast<char*>(realloc(rbuf_, REDIS_IOBUF_LEN));
}

MasterConn::~MasterConn() {
  free(rbuf_);
}

pink::ReadStatus MasterConn::ReadRaw(uint32_t count) {
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

pink::ReadStatus MasterConn::ReadHeader() {
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

pink::ReadStatus MasterConn::ReadBody(uint32_t body_length) {
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

int32_t MasterConn::FindNextSeparators(const std::string& content, int32_t pos) {
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

int32_t MasterConn::GetNextNum(const std::string& content, int32_t left_pos, int32_t right_pos, long* value) {
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
pink::ReadStatus MasterConn::ParseRedisRESPArray(const std::string& content, pink::RedisCmdArgsType* argv) {
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

void MasterConn::ResetStatus() {
  rbuf_len_ = 0;
  rbuf_cur_pos_ = 0;
}


pink::ReadStatus MasterConn::GetRequest() {
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

  if (type != kTypePortAuth && type != kTypePortBinlog) {
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
  if (type == kTypePortAuth) {
    if ((status = ParseRedisRESPArray(body, &argv)) != pink::kOk) {
      LOG(INFO) << "Type auth ParseRedisRESPArray error";
      return status;
    }
    if (!ProcessAuth(argv)) {
      return pink::kDealError;
    }
  } else if (type == kTypePortBinlog) {
    PortBinlogItem item;
    if (!PortBinlogTransverter::PortBinlogDecode(PortTypeFirst, body, &item)) {
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

pink::WriteStatus MasterConn::SendReply() {
  return pink::kWriteAll;
}

void MasterConn::TryResizeBuffer() {
}

bool MasterConn::ProcessAuth(const pink::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }

  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_port->sid())) {
      is_authed_ = true;
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: "
                << g_pika_port->sid() << ", Master auth server id: " << argv[1];
      return true;
    }
  }

  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: "
            << g_pika_port->sid() << " Master auth server id: " << argv[1];

  return false;
}

bool MasterConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item) {
  if (!is_authed_) {
    LOG(INFO) << "Need Auth First";
    return false;
  } else if (argv.empty()) {
    return false;
  }

  std::string key(" ");
  if (1 < argv.size()) {
    key = argv[1];
  }
  int ret = g_pika_port->SendRedisCommand(binlog_item.content(), key);
  if (ret != 0) {
    DLOG(WARNING) << "send redis command:" << binlog_item.ToString() << ", ret:%d" << ret;
  }

  return true;
}

