// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_CONN_H_
#define PIKA_REPL_CLIENT_CONN_H_

#include <vector>
#include <string>

#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"

class PikaReplClientConn: public pink::RedisConn {
 public:
  struct TaskArg {
    std::string argv;
    int port;
    TaskArg(const std::string _argv, int _port) : argv(_argv), port(_port) {
    }
  };

  PikaReplClientConn(int fd, const std::string& ip_port, pink::Thread *thread, void* worker_specific_data);
  virtual ~PikaReplClientConn() = default;

  static void DoReplClinetTask(void* arg);

  void AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) override;
  int DealMessage(const pink::RedisCmdArgsType& argv, std::string* response) override;
 private:
};

#endif
