// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_CONN_H_
#define PIKA_REPL_SERVER_CONN_H_

#include <string>

#include "pink/include/pb_conn.h"
#include "pink/include/redis_parser.h"
#include "pink/include/pink_thread.h"

#include "src/pika_inner_message.pb.h"
#include "include/pika_binlog_parser.h"

class PikaReplServerThread;

class PikaReplServerConn: public pink::PbConn {
 public:
  PikaReplServerConn(int fd, std::string ip_port, pink::Thread* thread, void* worker_specific_data, pink::PinkEpoll* epoll);
  virtual ~PikaReplServerConn();

  int DealMessage();
  bool ProcessAuth(const pink::RedisCmdArgsType& argv);
  bool ProcessBinlogData(const pink::RedisCmdArgsType& argv, const BinlogItem& binlog_item);

  BinlogHeader binlog_header_;
  BinlogItem binlog_item_;
 private:
  static int ParserDealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
  int HandleMetaSyncRequest(const InnerMessage::InnerRequest& req);
  int HandleTrySync(const InnerMessage::InnerRequest& req);
  int HandleBinlogSync(const InnerMessage::InnerRequest& req);

  bool is_authed_;
  pink::RedisParser redis_parser_;
  PikaBinlogParser binlog_parser_;

  PikaReplServerThread* binlog_receiver_;
};

#endif  // INCLUDE_PIKA_REPL_SERVER_CONN_H_
