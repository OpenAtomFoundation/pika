// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_repl_client_conn.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;

PikaReplClientConn::PikaReplClientConn(int fd,
                               const std::string& ip_port,
                               pink::Thread* thread,
                               void* worker_specific_data)
      : RedisConn(fd, ip_port, thread, nullptr, pink::HandleType::kSynchronous) {
}

void DoReplClientTadk(void* arg) {
  PikaReplClientConn::TaskArg* info = reinterpret_cast<PikaReplClientConn::TaskArg*>(arg);
  //  std::string& argv = info->argv;
  //  int port = info->port;
  delete info;
}

void PikaReplClientConn::AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) {
  TaskArg* arg = new TaskArg(argvs[0][0], 12000);
  g_pika_server->Schedule(&DoReplClientTadk, arg);
}

int PikaReplClientConn::DealMessage(const pink::RedisCmdArgsType& argv, std::string* response) {
  return 0;
}
