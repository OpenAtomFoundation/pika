// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef PIKA_CONSENSUS_H_
#define PIKA_CONSENSUS_H_

#include <utility>

#include "include/pika_binlog_transverter.h"
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"
#include "pstd/include/env.h"

class Context : public pstd::noncopyable {
 public:
  Context(std::string path);

  pstd::Status Init();
  // RWLock should be held when access members.
  pstd::Status StableSave();
  void PrepareUpdateAppliedIndex(const LogOffset& offset);
  void UpdateAppliedIndex(const LogOffset& offset);
  void Reset(const LogOffset& offset);

  std::shared_mutex rwlock_;
  LogOffset applied_index_;
  SyncWindow applied_win_;

  std::string ToString() {
    std::stringstream tmp_stream;
    std::shared_lock l(rwlock_);
    tmp_stream << "  Applied_index " << applied_index_.ToString() << "\r\n";
    tmp_stream << "  Applied window " << applied_win_.ToStringStatus();
    return tmp_stream.str();
  }

 private:
  std::string path_;
  std::unique_ptr<pstd::RWFile> save_;
};

class SyncProgress {
 public:
  SyncProgress() = default;
  ~SyncProgress() = default;
  std::shared_ptr<SlaveNode> GetSlaveNode(const std::string& ip, int port);
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> GetAllSlaveNodes();
  std::unordered_map<std::string, LogOffset> GetAllMatchIndex();
  pstd::Status AddSlaveNode(const std::string& ip, int port, const std::string& db_name, uint32_t slot_id,
                      int session_id);
  pstd::Status RemoveSlaveNode(const std::string& ip, int port);
  pstd::Status Update(const std::string& ip, int port, const LogOffset& start, const LogOffset& end,
                LogOffset* committed_index);
  int SlaveSize();

 private:

  std::shared_mutex rwlock_;
  std::unordered_map<std::string, std::shared_ptr<SlaveNode>> slaves_;
  std::unordered_map<std::string, LogOffset> match_index_;
};

class MemLog {
 public:
  struct LogItem {
    LogItem(const LogOffset& _offset, std::shared_ptr<Cmd> _cmd_ptr, std::shared_ptr<PikaClientConn> _conn_ptr,
            std::shared_ptr<std::string> _resp_ptr)
        : offset(_offset), cmd_ptr(std::move(_cmd_ptr)), conn_ptr(std::move(_conn_ptr)), resp_ptr(std::move(_resp_ptr)) {}
    LogOffset offset;
    std::shared_ptr<Cmd> cmd_ptr;
    std::shared_ptr<PikaClientConn> conn_ptr;
    std::shared_ptr<std::string> resp_ptr;
  };

  MemLog();
  int Size();
  void AppendLog(const LogItem& item) {
    std::lock_guard lock(logs_mu_);
    logs_.push_back(item);
    last_offset_ = item.offset;
  }
  pstd::Status PurgeLogs(const LogOffset& offset, std::vector<LogItem>* logs);
  pstd::Status GetRangeLogs(int start, int end, std::vector<LogItem>* logs);
  pstd::Status TruncateTo(const LogOffset& offset);

  void Reset(const LogOffset& offset);

  LogOffset last_offset() {
    std::lock_guard lock(logs_mu_);
    return last_offset_;
  }
  void SetLastOffset(const LogOffset& offset) {
    std::lock_guard lock(logs_mu_);
    last_offset_ = offset;
  }
  bool FindLogItem(const LogOffset& offset, LogOffset* found_offset);

 private:
  int InternalFindLogByBinlogOffset(const LogOffset& offset);
  int InternalFindLogByLogicIndex(const LogOffset& offset);
  pstd::Mutex logs_mu_;
  std::vector<LogItem> logs_;
  LogOffset last_offset_;
};

class ConsensusCoordinator {
 public:
  ConsensusCoordinator(const std::string& db_name, uint32_t slot_id);
  ~ConsensusCoordinator();
  // since it is invoked in constructor all locks not hold
  void Init();
  // invoked by dbsync process
  pstd::Status Reset(const LogOffset& offset);

  pstd::Status ProposeLog(const std::shared_ptr<Cmd>& cmd_ptr);
  pstd::Status UpdateSlave(const std::string& ip, int port, const LogOffset& start, const LogOffset& end);
  pstd::Status AddSlaveNode(const std::string& ip, int port, int session_id);
  pstd::Status RemoveSlaveNode(const std::string& ip, int port);
  void UpdateTerm(uint32_t term);
  uint32_t term();
  pstd::Status CheckEnoughFollower();

  // invoked by follower
  pstd::Status ProcessLeaderLog(const std::shared_ptr<Cmd>& cmd_ptr, const BinlogItem& attribute);

  // Negotiate
  pstd::Status LeaderNegotiate(const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints);
  pstd::Status FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset);

  SyncProgress& SyncPros() { return sync_pros_; }
  std::shared_ptr<StableLog> StableLogger() { return stable_logger_; }
  std::shared_ptr<MemLog> MemLogger() { return mem_logger_; }

  LogOffset committed_index() {
    std::lock_guard lock(index_mu_);
    return committed_index_;
  }

  LogOffset applied_index() {
    std::shared_lock lock(context_->rwlock_);
    return context_->applied_index_;
  }

  std::shared_ptr<Context> context() { return context_; }

  // redis parser cb
  struct CmdPtrArg {
    CmdPtrArg(std::shared_ptr<Cmd> ptr) : cmd_ptr(std::move(ptr)) {}
    std::shared_ptr<Cmd> cmd_ptr;
  };
  static int InitCmd(net::RedisParser* parser, const net::RedisCmdArgsType& argv);

  std::string ToStringStatus() {
    std::stringstream tmp_stream;
    {
      std::lock_guard lock(index_mu_);
      tmp_stream << "  Committed_index: " << committed_index_.ToString() << "\r\n";
    }
    tmp_stream << "  Context: "
               << "\r\n"
               << context_->ToString();
    {
      std::shared_lock lock(term_rwlock_);
      tmp_stream << "  Term: " << term_ << "\r\n";
    }
    tmp_stream << "  Mem_logger size: " << mem_logger_->Size() << " last offset "
               << mem_logger_->last_offset().ToString() << "\r\n";
    tmp_stream << "  Stable_logger first offset " << stable_logger_->first_offset().ToString() << "\r\n";
    LogOffset log_status;
    stable_logger_->Logger()->GetProducerStatus(&(log_status.b_offset.filenum), &(log_status.b_offset.offset),
                                                &(log_status.l_offset.term), &(log_status.l_offset.index));
    tmp_stream << "  Physical Binlog Status: " << log_status.ToString() << "\r\n";
    return tmp_stream.str();
  }

 private:
  pstd::Status ScheduleApplyLog(const LogOffset& committed_index);
  pstd::Status ScheduleApplyFollowerLog(const LogOffset& committed_index);
  bool MatchConsensusLevel();
  pstd::Status TruncateTo(const LogOffset& offset);

  pstd::Status InternalAppendLog(const std::shared_ptr<Cmd>& cmd_ptr);
  pstd::Status InternalAppendBinlog(const std::shared_ptr<Cmd>& cmd_ptr);
  void InternalApply(const MemLog::LogItem& log);
  void InternalApplyFollower(const MemLog::LogItem& log);

  pstd::Status GetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset);
  pstd::Status GetBinlogOffset(const BinlogOffset& start_offset, const BinlogOffset& end_offset,
                         std::vector<LogOffset>* log_offset);
  pstd::Status FindBinlogFileNum(const std::map<uint32_t, std::string>& binlogs, uint64_t target_index, uint32_t start_filenum,
                           uint32_t* founded_filenum);
  pstd::Status FindLogicOffsetBySearchingBinlog(const BinlogOffset& hint_offset, uint64_t target_index,
                                          LogOffset* found_offset);
  pstd::Status FindLogicOffset(const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset);
  pstd::Status GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints);

  // keep members in this class works in order
  pstd::Mutex order_mu_;

  pstd::Mutex index_mu_;
  LogOffset committed_index_;

  std::shared_ptr<Context> context_;

  std::shared_mutex term_rwlock_;
  uint32_t term_ = 0;

  std::string db_name_;
  uint32_t slot_id_ = 0;

  SyncProgress sync_pros_;
  std::shared_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;
};
#endif  // INCLUDE_PIKA_CONSENSUS_H_
