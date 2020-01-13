// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_H_
#define PIKA_REPL_SERVER_H_

#include "pink/include/thread_pool.h"

#include <vector>

#include "include/pika_command.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_server_thread.h"

struct ReplServerTaskArg {
  std::shared_ptr<InnerMessage::InnerRequest> req;
  std::shared_ptr<pink::PbConn> conn;
  ReplServerTaskArg(std::shared_ptr<InnerMessage::InnerRequest> _req, std::shared_ptr<pink::PbConn> _conn)
      : req(_req), conn(_conn) {}
};

class PikaReplServer {
 public:
  PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval);
  ~PikaReplServer();

  int Start();
  int Stop();

  slash::Status SendSlaveBinlogChips(const std::string& ip, int port, const std::vector<WriteTask>& tasks);
  void BuildBinlogOffset(const LogOffset& offset, InnerMessage::BinlogOffset* boffset);
  void BuildBinlogSyncResp(const std::vector<WriteTask>& tasks, InnerMessage::InnerResponse* resp);
  slash::Status Write(const std::string& ip, const int port, const std::string& msg);

  void Schedule(pink::TaskFunc func, void* arg);
  void UpdateClientConnMap(const std::string& ip_port, int fd);
  void RemoveClientConn(int fd);
  void KillAllConns();

 private:
  pink::ThreadPool* server_tp_;
  PikaReplServerThread* pika_repl_server_thread_;

  pthread_rwlock_t client_conn_rwlock_;
  std::map<std::string, int> client_conn_map_;
};

#endif
