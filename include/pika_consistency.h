// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef PIKA_CONSISTENCY_H_
#define PIKA_CONSISTENCY_H_

#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"

class SyncProgress {
 public:
  SyncProgress();
  ~SyncProgress();
  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();
  Status AddSlaveNode(const std::string& ip, int port,
      const std::string& table_name, uint32_t partition_id, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);
  int SlaveSize();

 private:
  pthread_rwlock_t rwlock_;
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves_;
};

class ConsistencyCoordinator {
 public:
  struct LogItem {
    LogItem(
        BinlogOffset _offset,
        std::shared_ptr<Cmd> _cmd_ptr,
        std::shared_ptr<PikaClientConn> _conn_ptr,
        std::shared_ptr<std::string> _resp_ptr)
      : offset(_offset), cmd_ptr(_cmd_ptr), conn_ptr(_conn_ptr), resp_ptr(_resp_ptr) {
    }
    BinlogOffset offset;
    std::shared_ptr<Cmd> cmd_ptr;
    std::shared_ptr<PikaClientConn> conn_ptr;
    std::shared_ptr<std::string> resp_ptr;
  };

  ConsistencyCoordinator(const std::string& table_name, uint32_t partition_id);
  Status ProposeLog(
      const BinlogOffset& offset,
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status ScheduleApplyLog();
  Status CheckEnoughFollower();
  Status UpdateMatchIndex(const std::string& ip, int port, const BinlogOffset& offset);

  Status AddSlaveNode(const std::string& ip, int port, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);

  Status AddFollower(const std::string& ip, int port);
  Status RemoveFollower(const std::string& ip, int port);

  SyncProgress& SyncPros() {
    return sync_pros_;
  }
  std::shared_ptr<StableLog> StableLogger() {
    return stable_logger_;
  }

  size_t LogsSize();

 private:
  int InternalFindLogIndex();
  void InternalUpdateCommittedIndex();
  bool InternalMatchConsistencyLevel();
  int InternalPurdgeLog(std::vector<LogItem>* logs);
  void InternalApply(const LogItem& log);
  void InternalApplyStale(const LogItem& log);

  slash::Mutex logs_mu_;
  std::vector<LogItem> logs_;
  slash::Mutex index_mu_;
  std::unordered_map<std::string, BinlogOffset> match_index_;
  BinlogOffset committed_index_;

  std::string table_name_;
  uint32_t partition_id_;

  SyncProgress sync_pros_;
  std::shared_ptr<StableLog> stable_logger_;
};
#endif  // INCLUDE_PIKA_CONSISTENCY_H_
