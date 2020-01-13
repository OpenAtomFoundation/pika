// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_BGWROKER_H_
#define PIKA_REPL_BGWROKER_H_

#include <memory>
#include <string>

#include "pink/include/pb_conn.h"
#include "pink/include/bg_thread.h"
#include "pink/include/thread_pool.h"

#include "src/pika_inner_message.pb.h"

#include "include/pika_command.h"
#include "include/pika_binlog_transverter.h"

class PikaReplBgWorker {
 public:
  explicit PikaReplBgWorker(int queue_size);
  int StartThread();
  int StopThread();
  void Schedule(pink::TaskFunc func, void* arg);
  void QueueClear();
  static void HandleBGWorkerWriteBinlog(void* arg);
  static void HandleBGWorkerWriteDB(void* arg);

  BinlogItem binlog_item_;
  pink::RedisParser redis_parser_;
  LogOffset offset_;
  std::string ip_port_;
  std::string table_name_;
  uint32_t partition_id_;

 private:
  pink::BGThread bg_thread_;
  static int HandleWriteBinlog(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
  static void ParseBinlogOffset(const InnerMessage::BinlogOffset pb_offset, LogOffset* offset);
};

#endif  // PIKA_REPL_BGWROKER_H_
