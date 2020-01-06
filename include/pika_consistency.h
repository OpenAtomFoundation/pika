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
  std::unordered_map<std::string, LogOffset> GetAllMatchIndex();
  Status AddSlaveNode(const std::string& ip, int port,
      const std::string& table_name, uint32_t partition_id, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);
  Status Update(const std::string& ip, int port, const LogOffset& start,
      const LogOffset& end, LogOffset* committed_index);
  int SlaveSize();

 private:
  LogOffset InternalCalCommittedIndex(
      std::unordered_map<std::string, LogOffset> match_index);

  pthread_rwlock_t rwlock_;
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves_;
  std::unordered_map<std::string, LogOffset> match_index_;
};

class MemLog {
 public:
  struct LogItem {
    LogItem(
        LogOffset _offset,
        std::shared_ptr<Cmd> _cmd_ptr,
        std::shared_ptr<PikaClientConn> _conn_ptr,
        std::shared_ptr<std::string> _resp_ptr)
      : offset(_offset), cmd_ptr(_cmd_ptr), conn_ptr(_conn_ptr), resp_ptr(_resp_ptr) {
    }
    LogOffset offset;
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
  Status PurdgeLogs(const LogOffset& offset, std::vector<LogItem>* logs);
  Status GetRangeLogs(int start, int end, std::vector<LogItem>* logs);

 private:
  int InternalFindLogIndex(const LogOffset& offset);
  slash::Mutex logs_mu_;
  std::vector<LogItem> logs_;
};

class ConsistencyCoordinator {
 public:
  ConsistencyCoordinator(const std::string& table_name, uint32_t partition_id);
  ~ConsistencyCoordinator();

  Status ProposeLog(
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status UpdateSlave(const std::string& ip, int port,
      const LogOffset& start, const LogOffset& end);
  Status AddSlaveNode(const std::string& ip, int port, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);
  void UpdateTerm(uint32_t term);

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
  Status ScheduleApplyLog(const LogOffset& committed_index);
  bool MatchConsistencyLevel();

  Status InternalAppendLog(std::shared_ptr<Cmd> cmd_ptr,
      LogOffset* log_offset);
  void InternalApply(const MemLog::LogItem& log);
  void InternalApplyStale(const MemLog::LogItem& log);
  bool InternalUpdateCommittedIndex(const LogOffset& slaves_committed_index,
      LogOffset* updated_committed_index);

  slash::Mutex index_mu_;
  LogOffset committed_index_;
  // LogOffset applied_index_;

  pthread_rwlock_t term_rwlock_;
  uint32_t term_;

  std::string table_name_;
  uint32_t partition_id_;

  SyncProgress sync_pros_;
  std::shared_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;
};
#endif  // INCLUDE_PIKA_CONSISTENCY_H_
