// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include "include/pika_command.h"

class PikaClientConn: public pink::RedisConn {
 public:
  struct BgTaskArg {
    std::shared_ptr<PikaClientConn> pcc;
    std::vector<pink::RedisCmdArgsType> redis_cmds;
    std::string* response;
  };

  // Auth related
  class AuthStat {
   public:
    void Init();
    bool IsAuthed(const std::shared_ptr<Cmd> cmd_ptr);
    bool ChecknUpdate(const std::string& arg);
   private:
    enum StatType {
      kNoAuthed = 0,
      kAdminAuthed,
      kLimitAuthed,
    };
    StatType stat_;
  };

  PikaClientConn(int fd, std::string ip_port,
                 pink::Thread *server_thread,
                 pink::PinkEpoll* pink_epoll,
                 const pink::HandleType& handle_type);
  virtual ~PikaClientConn() {}

  void AsynProcessRedisCmds(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response) override;

  void BatchExecRedisCmd(const std::vector<pink::RedisCmdArgsType>& argvs, std::string* response);
  int DealMessage(const pink::RedisCmdArgsType& argv, std::string* response);
  static void DoBackgroundTask(void* arg);

  bool IsPubSub() { return is_pubsub_; }
  void SetIsPubSub(bool is_pubsub) { is_pubsub_ = is_pubsub; }
  void SetCurrentTable(const std::string& table_name) {current_table_ = table_name;}

  pink::ServerThread* server_thread() {
    return server_thread_;
  }

  AuthStat& auth_stat() {
    return auth_stat_;
  }

 private:
  pink::ServerThread* const server_thread_;
  std::string current_table_;
  bool is_pubsub_;

  std::string DoCmd(const PikaCmdArgsType& argv, const std::string& opt);

  void ProcessSlowlog(const PikaCmdArgsType& argv, uint64_t start_us);
  void ProcessMonitor(const PikaCmdArgsType& argv);

  AuthStat auth_stat_;
};

struct ClientInfo {
  int fd;
  std::string ip_port;
  int64_t last_interaction;
  std::shared_ptr<PikaClientConn> conn;
};

extern bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs);
extern bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs);

#endif
