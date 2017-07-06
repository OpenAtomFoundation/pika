// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>
#include <atomic>

#include "pink/include/redis_conn.h"
#include "pink/include/pink_thread.h"
#include "pika_command.h"

class PikaWorkerSpecificData;

class PikaClientConn: public pink::RedisConn {
 public:
  PikaClientConn(int fd, std::string ip_port, pink::ServerThread *server_thread,
                 void* worker_specific_data);
  virtual ~PikaClientConn() {}
  virtual int DealMessage();

 private:
  pink::ServerThread* const server_thread_;
  CmdTable* const cmds_table_;

  std::string DoCmd(const std::string& opt);
  std::string RestoreArgs();

  // Auth related
  class AuthStat {
   public:
    void Init();
    bool IsAuthed(const CmdInfo* const cinfo_ptr);
    bool ChecknUpdate(const std::string& arg);
   private:
    enum StatType {
      kNoAuthed = 0,
      kAdminAuthed,
      kLimitAuthed,
    };
    StatType stat_;
  };
  AuthStat auth_stat_;
};

struct ClientInfo {
  int fd;
  std::string ip_port;
  long last_interaction;
  PikaClientConn* conn;
};

#endif
