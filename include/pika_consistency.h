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
  Status Update(const std::string& ip, int port, const BinlogOffset& start,
      const BinlogOffset& end, BinlogOffset* committed_index);
  int SlaveSize();

 private:
  BinlogOffset InternalCalCommittedIndex();

  pthread_rwlock_t rwlock_;
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves_;
  slash::Mutex match_mu_;
  std::unordered_map<std::string, BinlogOffset> match_index_;
};

class MemLog {
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

  MemLog();
  int Size();
  void PushLog(const LogItem& item) {
    slash::MutexLock l_logs(&logs_mu_);
    logs_.push_back(item);
  }
  Status PurdgeLogs(BinlogOffset offset, std::vector<LogItem>* logs);
  Status GetRangeLogs(int start, int end, std::vector<LogItem>* logs);

 private:
  int InternalFindLogIndex(BinlogOffset offset);
  slash::Mutex logs_mu_;
  std::vector<LogItem> logs_;
};

class ConsistencyCoordinator {
 public:
  ConsistencyCoordinator(const std::string& table_name, uint32_t partition_id);

  Status ProposeLog(
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status UpdateSlave(const std::string& ip, int port,
      const BinlogOffset& start, const BinlogOffset& end);
  Status AddSlaveNode(const std::string& ip, int port, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);

  Status CheckEnoughFollower();
  SyncProgress& SyncPros() {
    return sync_pros_;
  }
  std::shared_ptr<StableLog> StableLogger() {
    return stable_logger_;
  }
  std::shared_ptr<MemLog> MemLogger() {
    return mem_logger_;
  }

 private:
  // Could del if impl raft
  Status AddFollower(const std::string& ip, int port);
  // not implement
  Status RemoveFollower(const std::string& ip, int port);
  Status ScheduleApplyLog();
  bool MatchConsistencyLevel();

  Status InternalPutBinlog(std::shared_ptr<Cmd> cmd_ptr,
      BinlogOffset* binlog_offset);
  void InternalApply(const MemLog::LogItem& log);
  void InternalApplyStale(const MemLog::LogItem& log);
  bool InternalUpdateCommittedIndex(const BinlogOffset& slaves_committed_index);

  slash::Mutex index_mu_;
  BinlogOffset committed_index_;

  std::string table_name_;
  uint32_t partition_id_;

  SyncProgress sync_pros_;
  std::shared_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;
};
#endif  // INCLUDE_PIKA_CONSISTENCY_H_
