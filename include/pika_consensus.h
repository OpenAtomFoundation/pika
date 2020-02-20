// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef PIKA_CONSENSUS_H_
#define PIKA_CONSENSUS_H_

#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_slave_node.h"
#include "include/pika_stable_log.h"
#include "include/pika_binlog_transverter.h"
#include "slash/include/env.h"

class Context {
 public:
  Context(const std::string path);
  ~Context();

  Status Init();
  // RWLock should be held when access members.
  Status StableSave();
  void PrepareUpdateAppliedIndex(const LogOffset& offset);
  void UpdateAppliedIndex(const LogOffset& offset);
  void Reset(const LogOffset& applied_index);

  pthread_rwlock_t rwlock_;
  LogOffset applied_index_;
  SyncWindow applied_win_;

  std::string ToString() {
    std::stringstream tmp_stream;
    slash::RWLock l(&rwlock_, false);
    tmp_stream << "  Applied_index " << applied_index_.ToString() << "\r\n";
    tmp_stream << "  Applied window " << applied_win_.ToStringStatus();
    return tmp_stream.str();
  }

 private:
  std::string path_;
  slash::RWFile *save_;
  // No copying allowed;
  Context(const Context&);
  void operator=(const Context&);
};

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
  void AppendLog(const LogItem& item) {
    slash::MutexLock l_logs(&logs_mu_);
    logs_.push_back(item);
    last_offset_ = item.offset;
  }
  Status PurdgeLogs(const LogOffset& offset, std::vector<LogItem>* logs);
  Status GetRangeLogs(int start, int end, std::vector<LogItem>* logs);
  Status TruncateTo(const LogOffset& offset);

  void  Reset(const LogOffset& offset);

  LogOffset last_offset() {
    slash::MutexLock l_logs(&logs_mu_);
    return last_offset_;
  }
  void SetLastOffset(const LogOffset& offset) {
    slash::MutexLock l_logs(&logs_mu_);
    last_offset_ = offset;
  }
  bool FindLogItem(const LogOffset& offset, LogOffset* found_offset);

 private:
  int InternalFindLogIndex(const LogOffset& offset);
  slash::Mutex logs_mu_;
  std::vector<LogItem> logs_;
  LogOffset last_offset_;
};

class ConsensusCoordinator {
 public:
  ConsensusCoordinator(const std::string& table_name, uint32_t partition_id);
  ~ConsensusCoordinator();
  // since it is invoked in constructor all locks not hold
  void Init();
  // invoked by dbsync process
  Status Reset(const LogOffset& offset);

  Status ProposeLog(
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status UpdateSlave(const std::string& ip, int port,
      const LogOffset& start, const LogOffset& end);
  Status AddSlaveNode(const std::string& ip, int port, int session_id);
  Status RemoveSlaveNode(const std::string& ip, int port);
  void UpdateTerm(uint32_t term);
  uint32_t term();
  Status CheckEnoughFollower();

  // invoked by follower
  Status ProcessLeaderLog(std::shared_ptr<Cmd> cmd_ptr,
      const BinlogItem& attribute);
  Status ProcessLocalUpdate(const LogOffset& leader_commit);

  // Negotiate
  Status LeaderNegotiate(
      const LogOffset& f_last_offset, bool* reject, std::vector<LogOffset>* hints);
  Status FollowerNegotiate(const std::vector<LogOffset>& hints, LogOffset* reply_offset);

  SyncProgress& SyncPros() {
    return sync_pros_;
  }
  std::shared_ptr<StableLog> StableLogger() {
    return stable_logger_;
  }
  std::shared_ptr<MemLog> MemLogger() {
    return mem_logger_;
  }

  LogOffset committed_index() {
    slash::MutexLock l(&index_mu_);
    return committed_index_;
  }

  LogOffset applied_index() {
    slash::RWLock l(&context_->rwlock_, false);
    return context_->applied_index_;
  }

  std::shared_ptr<Context> context() {
    return context_;
  }

  // redis parser cb
  struct CmdPtrArg {
    CmdPtrArg(std::shared_ptr<Cmd> ptr) : cmd_ptr(ptr) {
    }
    std::shared_ptr<Cmd> cmd_ptr;
  };
  static int InitCmd(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);

  std::string ToStringStatus() {
    std::stringstream tmp_stream;
    {
      slash::MutexLock l(&index_mu_);
      tmp_stream << "  Committed_index: " << committed_index_.ToString() << "\r\n";
    }
    tmp_stream << "  Contex: " << "\r\n" << context_->ToString();
    {
      slash::RWLock l(&term_rwlock_, false);
      tmp_stream << "  Term: " << term_ << "\r\n";
    }
    tmp_stream << "  Mem_logger size: " << mem_logger_->Size() <<
      " last offset " << mem_logger_->last_offset().ToString() << "\r\n";
    tmp_stream << "  Stable_logger first offset " << stable_logger_->first_offset().ToString() << "\r\n";
    LogOffset log_status;
    stable_logger_->Logger()->GetProducerStatus(
        &(log_status.b_offset.filenum), &(log_status.b_offset.offset),
        &(log_status.l_offset.term), &(log_status.l_offset.index));
    tmp_stream << "  Physical Binlog Status: " << log_status.ToString();
    return tmp_stream.str();
  }

 private:
  Status ScheduleApplyLog(const LogOffset& committed_index);
  Status ScheduleApplyFollowerLog(const LogOffset& committed_index);
  bool MatchConsensusLevel();
  Status TruncateTo(const LogOffset& offset);

  Status InternalAppendLog(const BinlogItem& item,
      std::shared_ptr<Cmd> cmd_ptr,
      std::shared_ptr<PikaClientConn> conn_ptr,
      std::shared_ptr<std::string> resp_ptr);
  Status InternalAppendBinlog(const BinlogItem& item,
      std::shared_ptr<Cmd> cmd_ptr,
      LogOffset* log_offset);
  void InternalApply(const MemLog::LogItem& log);
  void InternalApplyFollower(const MemLog::LogItem& log);
  bool InternalUpdateCommittedIndex(const LogOffset& slaves_committed_index,
      LogOffset* updated_committed_index);

  Status GetBinlogOffset(const BinlogOffset& start_offset, LogOffset* log_offset);
  Status GetBinlogOffset(
      const BinlogOffset& start_offset,
      const BinlogOffset& end_offset, std::vector<LogOffset>* log_offset);
  Status FindBinlogFileNum(
      const std::map<uint32_t, std::string> binlogs,
      uint64_t target_index, uint32_t start_filenum,
      uint32_t* founded_filenum);
  Status FindLogicOffsetBySearchingBinlog(
      const BinlogOffset& hint_offset, uint64_t target_index, LogOffset* found_offset);
  Status FindLogicOffset(
      const BinlogOffset& start_offset, uint64_t target_index, LogOffset* found_offset);
  Status GetLogsBefore(const BinlogOffset& start_offset, std::vector<LogOffset>* hints);

  slash::Mutex index_mu_;
  LogOffset committed_index_;

  std::shared_ptr<Context> context_;

  pthread_rwlock_t term_rwlock_;
  uint32_t term_;

  std::string table_name_;
  uint32_t partition_id_;

  SyncProgress sync_pros_;
  std::shared_ptr<StableLog> stable_logger_;
  std::shared_ptr<MemLog> mem_logger_;
};
#endif  // INCLUDE_PIKA_CONSENSUS_H_
