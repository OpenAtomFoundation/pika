// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include <utility>

#include "pstd/include/pstd_coding.h"
#include "pstd/include/pstd_string.h"

#include "binlog_receiver_thread.h"
#include "binlog_transverter.h"
#include "const.h"
#include "master_conn.h"
#include "pika_port.h"

extern PikaPort* g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port, void* worker_specific_data)
    : NetConn(fd, std::move(ip_port), nullptr),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_size_(REDIS_IOBUF_LEN),
      rbuf_cur_pos_(0),
      is_authed_(false) {
  binlog_receiver_ = reinterpret_cast<BinlogReceiverThread*>(worker_specific_data);
  rbuf_ = static_cast<char*>(realloc(rbuf_, REDIS_IOBUF_LEN));
}

MasterConn::~MasterConn() { free(rbuf_); }

net::ReadStatus MasterConn::ReadRaw(uint32_t count) {
  if (rbuf_cur_pos_ + count > rbuf_size_) {
    return net::kFullError;
  }
  int32_t nread = read(fd(), rbuf_ + rbuf_len_, count - (rbuf_len_ - rbuf_cur_pos_));
  if (nread == -1) {
    if (errno == EAGAIN) {
      return net::kReadHalf;
    } else {
      return net::kReadError;
    }
  } else if (nread == 0) {
    return net::kReadClose;
  }

  rbuf_len_ += nread;
  if (rbuf_len_ - rbuf_cur_pos_ != count) {
    return net::kReadHalf;
  }
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadHeader() {
  if (rbuf_len_ >= HEADER_LEN) {
    return net::kReadAll;
  }

  net::ReadStatus status = ReadRaw(HEADER_LEN);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += HEADER_LEN;
  return net::kReadAll;
}

net::ReadStatus MasterConn::ReadBody(uint32_t body_length) {
  if (rbuf_len_ == HEADER_LEN + body_length) {
    return net::kReadAll;
  } else if (rbuf_len_ > HEADER_LEN + body_length) {
    LOG(INFO) << "rbuf_len_ larger than sum of header length (6 Byte)"
              << " and body_length, rbuf_len_: " << rbuf_len_ << ", body_length: " << body_length;
  }

  net::ReadStatus status = ReadRaw(body_length);
  if (status != net::kReadAll) {
    return status;
  }
  rbuf_cur_pos_ += body_length;
  return net::kReadAll;
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
  if (pstd::string2int(content.data() + left_pos + 1, right_pos - left_pos - 2, value) != 0) {
    return 0;
  }
  return -1;
}

// RedisRESPArray : *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
net::ReadStatus MasterConn::ParseRedisRESPArray(const std::string& content, net::RedisCmdArgsType* argv) {
  int32_t pos = 0;
  int32_t next_parse_pos = 0;
  int32_t content_len = content.size();
  long multibulk_len = 0;
  long bulk_len = 0;
  if (content.empty() || content[0] != '*') {
    LOG(INFO) << "Content empty() or the first character of the redis protocol string not equal '*'";
    return net::kParseError;
  }
  pos = FindNextSeparators(content, next_parse_pos);
  if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &multibulk_len) != -1) {
    next_parse_pos = pos + 1;
  } else {
    LOG(INFO) << "Find next separators error or get next num error";
    return net::kParseError;
  }

  // next_parst_pos          pos
  //        |-------   -------|
  //               |   |
  //               $3\r\nset\r\n
  //               012 3 4567 8

  argv->clear();
  while (multibulk_len != 0) {
    if (content[next_parse_pos] != '$') {
      LOG(INFO) << "The first charactor of the RESP type element not equal '$'";
      return net::kParseError;
    }

    bulk_len = -1;
    pos = FindNextSeparators(content, next_parse_pos);
    if (pos != -1 && GetNextNum(content, next_parse_pos, pos, &bulk_len) != -1) {
      if (pos + 1 + bulk_len + 2 > content_len) {
        return net::kParseError;
      } else {
        next_parse_pos = pos + 1;
        argv->emplace_back(content.data() + next_parse_pos, bulk_len);
        next_parse_pos = next_parse_pos + bulk_len + 2;
        multibulk_len--;
      }
    } else {
      LOG(INFO) << "Find next separators error or get next num error";
      return net::kParseError;
    }
  }
  if (content_len != next_parse_pos) {
    LOG(INFO) << "Incomplete parse";
    return net::kParseError;
  } else {
    return net::kOk;
  }
}

void MasterConn::ResetStatus() {
  rbuf_len_ = 0;
  rbuf_cur_pos_ = 0;
}

net::ReadStatus MasterConn::GetRequest() {
  // Read Header
  net::ReadStatus status;
  if ((status = ReadHeader()) != net::kReadAll) {
    return status;
  }

  // Process Header, get body length;
  uint16_t type = 0;
  uint32_t body_length = 0;
  std::string header(rbuf_, HEADER_LEN);
  pstd::GetFixed16(&header, &type);
  pstd::GetFixed32(&header, &body_length);

  if (type != kTypePortAuth && type != kTypePortBinlog) {
    LOG(INFO) << "Unrecognizable Type: " << type << " maybe identify binlog type error";
    return net::kParseError;
  }

  // Realloc buffer according header_len and body_length if need
  uint32_t needed_size = HEADER_LEN + body_length;
  if (rbuf_size_ < needed_size) {
    if (needed_size > REDIS_MAX_MESSAGE) {
      return net::kFullError;
    } else {
      rbuf_ = static_cast<char*>(realloc(rbuf_, needed_size));
      rbuf_size_ = needed_size;
    }
  }

  // Read Body
  if ((status = ReadBody(body_length)) != net::kReadAll) {
    return status;
  }

  net::RedisCmdArgsType argv;
  std::string body(rbuf_ + HEADER_LEN, body_length);
  if (type == kTypePortAuth) {
    if ((status = ParseRedisRESPArray(body, &argv)) != net::kOk) {
      LOG(INFO) << "Type auth ParseRedisRESPArray error";
      return status;
    }
    if (!ProcessAuth(argv)) {
      return net::kDealError;
    }
  } else if (type == kTypePortBinlog) {
    PortBinlogItem item;
    if (!PortBinlogTransverter::PortBinlogDecode(PortTypeFirst, body, &item)) {
      LOG(INFO) << "Binlog decode error: " << item.ToString();
      return net::kParseError;
    }
    if ((status = ParseRedisRESPArray(item.content(), &argv)) != net::kOk) {
      LOG(INFO) << "Type Binlog ParseRedisRESPArray error: " << item.ToString();
      return status;
    }
    if (!ProcessBinlogData(argv, item)) {
      return net::kDealError;
    }
  } else {
    LOG(INFO) << "Unrecognizable Type";
    return net::kParseError;
  }

  // Reset status
  ResetStatus();
  return net::kReadAll;
}

net::WriteStatus MasterConn::SendReply() { return net::kWriteAll; }

void MasterConn::TryResizeBuffer() {}

bool MasterConn::ProcessAuth(const net::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }

  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_port->sid())) {
      is_authed_ = true;
      LOG(INFO) << "BinlogReceiverThread AccessHandle succeeded, My server id: " << g_pika_port->sid()
                << ", Master auth server id: " << argv[1];
      return true;
    }
  }

  LOG(INFO) << "BinlogReceiverThread AccessHandle failed, My server id: " << g_pika_port->sid()
            << ", Master auth server id: " << argv[1];

  return false;
}

bool MasterConn::ProcessBinlogData(const net::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item) {
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
    LOG(WARNING) << "send redis command:" << binlog_item.ToString() << ", ret:" << ret;
  }

  return true;
}
