// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_REDIS_CONN_H_
#define NET_INCLUDE_REDIS_CONN_H_

#include <map>
#include <string>
#include <vector>

#include "net/include/net_conn.h"
#include "net/include/net_define.h"
#include "net/include/redis_parser.h"
#include "pstd/include/pstd_status.h"

namespace net {

using RedisCmdArgsType = std::vector<std::string>;

enum HandleType { kSynchronous, kAsynchronous };

class RedisConn : public NetConn {
 public:
  RedisConn(int32_t fd, const std::string& ip_port, Thread* thread, NetMultiplexer* net_mpx = nullptr,
            const HandleType& handle_type = kSynchronous, int32_t rbuf_max_len = REDIS_MAX_MESSAGE);
  ~RedisConn() override;

  ReadStatus GetRequest() override;
  WriteStatus SendReply() override;
  int32_t WriteResp(const std::string& resp) override;

  void TryResizeBuffer() override;
  void SetHandleType(const HandleType& handle_type);
  HandleType GetHandleType();

  virtual void ProcessRedisCmds(const std::vector<RedisCmdArgsType>& argvs, bool async, std::string* response);
  void NotifyEpoll(bool success);

  virtual int32_t DealMessage(const RedisCmdArgsType& argv, std::string* response) = 0;
  virtual const std::string& GetCurrentTable() = 0;

 private:
  static int32_t ParserDealMessageCb(RedisParser* parser, const RedisCmdArgsType& argv);
  static int32_t ParserCompleteCb(RedisParser* parser, const std::vector<RedisCmdArgsType>& argvs);
  ReadStatus ParseRedisParserStatus(RedisParserStatus status);

  HandleType handle_type_ = kSynchronous;

  char* rbuf_ = nullptr;
  int32_t rbuf_len_ = 0;
  int32_t rbuf_max_len_ = 0;
  int32_t msg_peak_ = 0;
  int32_t command_len_ = 0;

  uint32_t wbuf_pos_ = 0;
  std::string response_;

  // For Redis Protocol parser
  int32_t last_read_pos_ = -1;
  RedisParser redis_parser_;
  int32_t bulk_len_ = -1;
};

}  // namespace net
#endif  // NET_INCLUDE_REDIS_CONN_H_
