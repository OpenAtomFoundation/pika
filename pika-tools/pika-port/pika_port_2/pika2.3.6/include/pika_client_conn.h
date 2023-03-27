// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>
#include <atomic>

#include "net/include/redis_conn.h"
#include "net/include/net_thread.h"
#include "include/pika_command.h"

class PikaWorkerSpecificData;

class PikaClientConn: public net::RedisConn {
 public:
  PikaClientConn(int fd, std::string ip_port, net::ServerThread *server_thread,
                 void* worker_specific_data);
  virtual ~PikaClientConn() {}
  int DealMessage(PikaCmdArgsType& argv, std::string* response) override;

  bool IsPubSub() { return is_pubsub_; }
  void SetIsPubSub(bool is_pubsub) { is_pubsub_ = is_pubsub; }

 private:
  net::ServerThread* const server_thread_;
  CmdTable* const cmds_table_;
  bool is_pubsub_;

  std::string DoCmd(PikaCmdArgsType& argv, const std::string& opt);

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
  int64_t last_interaction;
  PikaClientConn* conn;
};

#endif
