// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "pink/include/redis_conn.h"

#include <stdlib.h>
#include <limits.h>

#include <string>
#include <sstream>

#include "slash/include/xdebug.h"
#include "slash/include/slash_string.h"

namespace pink {

RedisConn::RedisConn(const int fd,
                     const std::string& ip_port,
                     Thread* thread,
                     PinkEpoll* pink_epoll,
                     const HandleType& handle_type,
                     const int rbuf_max_len)
    : PinkConn(fd, ip_port, thread, pink_epoll),
      handle_type_(handle_type),
      rbuf_(nullptr),
      rbuf_len_(0),
      rbuf_max_len_(rbuf_max_len),
      msg_peak_(0),
      command_len_(0),
      wbuf_pos_(0),
      last_read_pos_(-1),
      bulk_len_(-1) {
  RedisParserSettings settings;
  settings.DealMessage = ParserDealMessageCb;
  settings.Complete = ParserCompleteCb;
  redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
  redis_parser_.data = this;
}

RedisConn::~RedisConn() {
  free(rbuf_);
}

ReadStatus RedisConn::ParseRedisParserStatus(RedisParserStatus status) {
  if (status == kRedisParserInitDone) {
    return kOk;
  } else if (status == kRedisParserHalf) {
    return kReadHalf;
  } else if (status == kRedisParserDone) {
    return kReadAll;
  } else if (status == kRedisParserError) {
    RedisParserError error_code = redis_parser_.get_error_code();
    switch (error_code) {
      case kRedisParserOk :
        return kReadError;
      case kRedisParserInitError :
        return kReadError;
      case kRedisParserFullError :
        return kFullError;
      case kRedisParserProtoError :
        return kParseError;
      case kRedisParserDealError :
        return kDealError;
      default :
        return kReadError;
    }
  } else {
    return kReadError;
  }
}

ReadStatus RedisConn::GetRequest() {
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
    if (new_size > rbuf_max_len_) {
      return kFullError;
    }
    rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
    if (rbuf_ == nullptr) {
      return kFullError;
    }
    rbuf_len_ = new_size;
  }

  nread = read(fd(), rbuf_ + next_read_pos, remain);
  if (nread == -1) {
    if (errno == EAGAIN) {
      nread = 0;
      return kReadHalf; // HALF
    } else {
      // error happened, close client
      return kReadError;
    }
  } else if (nread == 0) {
    // client closed, close client
    return kReadClose;
  }
  // assert(nread > 0);
  last_read_pos_ += nread;
  msg_peak_ = last_read_pos_;
  command_len_ += nread;
  if (command_len_ >= rbuf_max_len_) {
    log_info("close conn command_len %d, rbuf_max_len %d", command_len_, rbuf_max_len_);
    return kFullError;
  }

  int processed_len = 0;
  RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
      rbuf_ + next_read_pos, nread, &processed_len);
  ReadStatus read_status = ParseRedisParserStatus(ret);
  if (read_status == kReadAll || read_status == kReadHalf) {
    if (read_status == kReadAll) {
      command_len_ = 0;
    }
    last_read_pos_ = -1;
    bulk_len_ = redis_parser_.get_bulk_len();
  }
  if (!response_.empty()) {
    set_is_reply(true);
  }
  return read_status; // OK || HALF || FULL_ERROR || PARSE_ERROR
}

WriteStatus RedisConn::SendReply() {
  ssize_t nwritten = 0;
  size_t wbuf_len = response_.size();
  while (wbuf_len > 0) {
    nwritten = write(fd(), response_.data() + wbuf_pos_, wbuf_len - wbuf_pos_);
    if (nwritten <= 0) {
      break;
    }
    wbuf_pos_ += nwritten;
    if (wbuf_pos_ == wbuf_len) {
      // Have sended all response data
      if (wbuf_len > DEFAULT_WBUF_SIZE) {
        std::string buf;
        buf.reserve(DEFAULT_WBUF_SIZE);
        response_.swap(buf);
      }
      response_.clear();

      wbuf_len = 0;
      wbuf_pos_ = 0;
    }
  }
  if (nwritten == -1) {
    if (errno == EAGAIN) {
      return kWriteHalf;
    } else {
      // Here we should close the connection
      return kWriteError;
    }
  }
  if (wbuf_len == 0) {
    return kWriteAll;
  } else {
    return kWriteHalf;
  }
}

int RedisConn::WriteResp(const std::string& resp) {
  response_.append(resp);
  set_is_reply(true);
  return 0;
}

void RedisConn::TryResizeBuffer() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  int idletime = now.tv_sec - last_interaction().tv_sec;
  if (rbuf_len_ > REDIS_MBULK_BIG_ARG &&
      ((rbuf_len_ / (msg_peak_ + 1)) > 2 || idletime > 2)) {
    int new_size =
      ((last_read_pos_ + REDIS_IOBUF_LEN) / REDIS_IOBUF_LEN) * REDIS_IOBUF_LEN;
    if (new_size < rbuf_len_) {
      rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
      rbuf_len_ = new_size;
      log_info("Resize buffer to %d, last_read_pos_: %d\n",
               rbuf_len_, last_read_pos_);
    }
    msg_peak_ = 0;
  }
}

void RedisConn::SetHandleType(const HandleType& handle_type) {
  handle_type_ = handle_type;
}

HandleType RedisConn::GetHandleType() {
  return handle_type_;
}

void RedisConn::ProcessRedisCmds(const std::vector<RedisCmdArgsType>& argvs, bool async, std::string* response) {
}

void RedisConn::NotifyEpoll(bool success) {
  PinkItem ti(fd(), ip_port(), success ? kNotiEpolloutAndEpollin : kNotiClose);
  pink_epoll()->Register(ti, true);
}

int RedisConn::ParserDealMessageCb(RedisParser* parser, const RedisCmdArgsType& argv) {
  RedisConn* conn = reinterpret_cast<RedisConn*>(parser->data);
  if (conn->GetHandleType() == HandleType::kSynchronous) {
    return conn->DealMessage(argv, &(conn->response_));
  } else {
    return 0;
  }
}

int RedisConn::ParserCompleteCb(RedisParser* parser, const std::vector<RedisCmdArgsType>& argvs) {
  RedisConn* conn = reinterpret_cast<RedisConn*>(parser->data);
  bool async = conn->GetHandleType() == HandleType::kAsynchronous;
  conn->ProcessRedisCmds(argvs, async, &(conn->response_));
  return 0;
}

}  // namespace pink
