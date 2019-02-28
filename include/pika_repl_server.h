// Copyright (c) 2019-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_SERVER_H_
#define PIKA_REPL_SERVER_H_

#include "include/pika_repl_server_thread.h"

#include <vector>

#include "include/pika_repl_bgworker.h"
#include "include/pika_command.h"
#include "pika_binlog_transverter.h"

class PikaReplServer {
 public:
  PikaReplServer(const std::set<std::string>& ips, int port, int cron_interval);
  ~PikaReplServer();
  int Start();

  void ScheduleBinlogSyncTask(std::string table_partition,
                              const std::shared_ptr<InnerMessage::InnerRequest> req,
                              std::shared_ptr<pink::PbConn> conn,
                              void* req_private_data);

  void ScheduleMetaSyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
                            std::shared_ptr<pink::PbConn> conn,
                            void* req_private_data);

  void ScheduleTrySyncTask(const std::shared_ptr<InnerMessage::InnerRequest> req,
                           std::shared_ptr<pink::PbConn> conn,
                           void* req_private_data);

  void ScheduleDbTask(const std::string &key,
                      PikaCmdArgsType* argv,
                      BinlogItem* binlog_item);
 private:
  size_t GetHashIndex(std::string key);
  void UpdateNextAvail() {
    next_avail_ = (next_avail_ + 1) % bg_workers_.size();
  }

  PikaReplServerThread* pika_repl_server_thread_;
  std::vector<PikaReplBgWorker*> bg_workers_;
  int next_avail_;
  std::hash<std::string> str_hash;
};

#endif
