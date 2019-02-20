// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <string>
#include <memory>

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"
#include "pink/include/thread_pool.h"
#include "slash/include/slash_status.h"

#include "include/pika_partition.h"
#include "include/pika_binlog_reader.h"
#include "include/pika_repl_client_thread.h"

#define kBinlogSyncBatchNum 10

using slash::Status;

struct RmNode {
  std::string table_;
  uint32_t partition_;
  std::string ip_;
  int port_;
  RmNode(const std::string& table, int partition, const std::string& ip, int port) : table_(table), partition_(partition), ip_(ip), port_(port) {
  }
};

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();
  slash::Status Write(const std::string& ip, const int port, const std::string& msg);
  //void ThreadPollSchedule(pink::TaskFunc func, void*arg);
  int Start();
  Status AddBinlogReader(const RmNode& slave, std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);
  Status RemoveBinlogReader(const RmNode& slave);
  void RunStateMachine(const RmNode& slave);
  bool NeedToSendBinlog(const RmNode& slave);

  Status SendMetaSync();
  Status SendPartitionTrySync(const std::string& table_name,
                              uint32_t partition_id,
                              const BinlogOffset& boffset);

 private:
  PikaBinlogReader* NewPikaBinlogReader(std::shared_ptr<Binlog> logger, uint32_t filenum, uint64_t offset);

  Status TrySendSyncBinlog(const RmNode& slave);
  void BuildBinlogPb(const RmNode& slave, const std::string& msg, uint32_t filenum, uint64_t offset, InnerMessage::InnerRequest& request);

  Status BuildBinlogMsgFromFile(const RmNode& slave, std::string* scratch, uint32_t* filenum, uint64_t* offset);

  PikaReplClientThread* client_thread_;
  // keys of this map: table_partition_slaveip:port
  std::map<std::string, PikaBinlogReader*> slave_binlog_readers_;
};

#endif
