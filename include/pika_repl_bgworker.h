// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_BGWROKER_H_
#define PIKA_REPL_BGWROKER_H_

#include <memory>
#include <string>
#include <functional>
#include "net/include/bg_thread.h"
#include "net/include/pb_conn.h"
#include "net/include/thread_pool.h"

#include "pika_inner_message.pb.h"

#include "include/pika_binlog_transverter.h"
#include "include/pika_define.h"
#include "include/pika_command.h"

class PikaReplBgWorker {
 public:
  explicit PikaReplBgWorker(int queue_size);
  int StartThread();
  int StopThread();
  int TaskQueueSize() {
    int pri_size = 0;
    int qu_size = 0;
    bg_thread_.QueueSize(&pri_size, &qu_size);
    return pri_size + qu_size;
  }
  void Schedule(net::TaskFunc func, void* arg);
  void Schedule(net::TaskFunc func, void* arg, std::function<void()>& call_back);
  static void HandleBGWorkerWriteBinlog(void* arg);
  static void HandleBGWorkerWriteDB(void* arg);
  static void WriteDBInSyncWay(const std::shared_ptr<Cmd>& c_ptr);
  void SetThreadName(const std::string& thread_name) {
    bg_thread_.set_thread_name(thread_name);
  }
  BinlogItem binlog_item_;
  net::RedisParser redis_parser_;
  std::string ip_port_;
  std::string db_name_;

 private:
  net::BGThread bg_thread_;
  static int HandleWriteBinlog(net::RedisParser* parser, const net::RedisCmdArgsType& argv);
  static void ParseBinlogOffset(const InnerMessage::BinlogOffset& pb_offset, LogOffset* offset);
};

#endif  // PIKA_REPL_BGWROKER_H_
