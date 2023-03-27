// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_REDIS_CONN_H_
#define NET_INCLUDE_REDIS_CONN_H_

#include <map>
#include <vector>
#include <string>

#include "pstd/include/pstd_status.h"
#include "net/include/net_define.h"
#include "net/include/net_conn.h"
#include "net/include/redis_parser.h"

namespace net {

typedef std::vector<std::string> RedisCmdArgsType;

enum HandleType {
  kSynchronous,
  kAsynchronous
};

class RedisConn: public NetConn {
 public:
  RedisConn(const int fd,
            const std::string& ip_port,
            Thread* thread,
            NetEpoll* net_epoll = nullptr,
            const HandleType& handle_type = kSynchronous,
            const int rbuf_max_len = REDIS_MAX_MESSAGE);
  virtual ~RedisConn();

  virtual ReadStatus GetRequest();
  virtual WriteStatus SendReply();
  virtual int WriteResp(const std::string& resp);

  void TryResizeBuffer() override;
  void SetHandleType(const HandleType& handle_type);
  HandleType GetHandleType();

  virtual void ProcessRedisCmds(const std::vector<RedisCmdArgsType>& argvs, bool async, std::string* response);
  void NotifyEpoll(bool success);

  virtual int DealMessage(const RedisCmdArgsType& argv, std::string* response) = 0;

 private:
  static int ParserDealMessageCb(RedisParser* parser, const RedisCmdArgsType& argv);
  static int ParserCompleteCb(RedisParser* parser, const std::vector<RedisCmdArgsType>& argvs);
  ReadStatus ParseRedisParserStatus(RedisParserStatus status);

  HandleType handle_type_;

  char* rbuf_;
  int rbuf_len_;
  int rbuf_max_len_;
  int msg_peak_;
  int command_len_;

  uint32_t wbuf_pos_;
  std::string response_;

  // For Redis Protocol parser
  int last_read_pos_;
  RedisParser redis_parser_;
  long bulk_len_;
};

}  // namespace net
#endif  // NET_INCLUDE_REDIS_CONN_H_
