// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_H_
#define PIKA_REPL_SERVER_H_

#include "net/include/thread_pool.h"

#include <shared_mutex>
#include <utility>
#include <vector>

#include "include/pika_command.h"
#include "include/pika_repl_bgworker.h"
#include "include/pika_repl_server_thread.h"

struct ReplServerTaskArg {
  std::shared_ptr<InnerMessage::InnerRequest> req;
  std::shared_ptr<net::PbConn> conn;
  ReplServerTaskArg(std::shared_ptr<InnerMessage::InnerRequest> _req, std::shared_ptr<net::PbConn> _conn)
      : req(std::move(_req)), conn(std::move(_conn)) {}
};

class PikaReplServer {
 public:
  PikaReplServer(const std::set<std::string>& ips, int32_t port, int32_t cron_interval);
  ~PikaReplServer();

  int32_t Start();
  int32_t Stop();

  pstd::Status SendSlaveBinlogChips(const std::string& ip, int32_t port, const std::vector<WriteTask>& tasks);
  void BuildBinlogOffset(const LogOffset& offset, InnerMessage::BinlogOffset* boffset);
  void BuildBinlogSyncResp(const std::vector<WriteTask>& tasks, InnerMessage::InnerResponse* resp);
  pstd::Status Write(const std::string& ip, int32_t port, const std::string& msg);

  void Schedule(net::TaskFunc func, void* arg);
  void UpdateClientConnMap(const std::string& ip_port, int32_t fd);
  void RemoveClientConn(int32_t fd);
  void KillAllConns();

 private:
  std::unique_ptr<net::ThreadPool> server_tp_ = nullptr;
  std::unique_ptr<PikaReplServerThread> pika_repl_server_thread_ = nullptr;

  std::shared_mutex client_conn_rwlock_;
  std::map<std::string, int32_t> client_conn_map_;
};

#endif
