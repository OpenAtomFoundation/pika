// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_BGWROKER_H_
#define PIKA_REPL_BGWROKER_H_

#include <memory>
#include <string>

#include "pink/include/bg_thread.h"
#include "pink/include/pb_conn.h"

#include "src/pika_inner_message.pb.h"

#include "include/pika_command.h"
#include "include/pika_binlog_transverter.h"

class PikaReplBgWorker {
 public:
  explicit PikaReplBgWorker(int queue_size);
  void ScheduleRequest(const std::shared_ptr<InnerMessage::InnerRequest> req,
      std::shared_ptr<pink::PbConn> conn, void* req_private_data);
  void ScheduleWriteDb(PikaCmdArgsType* argv, BinlogItem* binlog_item);
  int StartThread();

  static void HandleMetaSyncRequest(void* arg);
  static void HandleBinlogSyncRequest(void* arg);
  static void HandleWriteDb(void* arg);

  BinlogItem binlog_item_;
  pink::RedisParser redis_parser_;
  std::string ip_port_;

 private:
  pink::BGThread bg_thread_;

  static int HandleWriteBinlog(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);

  struct ReplBgWorkerArg {
    const std::shared_ptr<InnerMessage::InnerRequest> req;
    std::shared_ptr<pink::PbConn> conn;
    void* req_private_data;
    PikaReplBgWorker* worker;
    ReplBgWorkerArg(const std::shared_ptr<InnerMessage::InnerRequest> _req, std::shared_ptr<pink::PbConn> _conn, void* _req_private_data, PikaReplBgWorker* _worker) : req(_req), conn(_conn), req_private_data(_req_private_data), worker(_worker) {
    }
  };

  struct WriteDbBgArg {
    PikaCmdArgsType *argv;
    BinlogItem* binlog_item;
    WriteDbBgArg(PikaCmdArgsType* _argv, BinlogItem* _binlog_item)
        : argv(_argv), binlog_item(_binlog_item) {
    }
    ~WriteDbBgArg() {
      delete argv;
      delete binlog_item;
    }
  };
};

#endif  // PIKA_REPL_BGWROKER_H_
