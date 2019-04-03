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
  void Schedule(pink::TaskFunc func, void* arg);
  void KillAllConns();

  static void HandleMetaSyncRequest(void* arg);
  static void HandleTrySyncRequest(void* arg);
  static void HandleDBSyncRequest(void* arg);
  static void HandleBinlogSyncAckRequest(void* arg);

 private:
  pink::ThreadPool* server_tp_;
  PikaReplServerThread* pika_repl_server_thread_;
};

#endif
