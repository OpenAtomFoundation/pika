// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_consistency.h"

#include "include/pika_conf.h"
#include "include/pika_server.h"
#include "include/pika_client_conn.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

/* SyncProgress */

SyncProgress::SyncProgress() {
  pthread_rwlock_init(&rwlock_, NULL);
}

SyncProgress::~SyncProgress() {
  pthread_rwlock_destroy(&rwlock_);
}

std::shared_ptr<SlaveNode> SyncProgress::GetSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  slash::RWLock l(&rwlock_, false);
  if (slaves_.find(slave_key) == slaves_.end()) {
    return nullptr;
  }
  return slaves_[slave_key];
}

std::unordered_map<std::string, std::shared_ptr<SlaveNode>> SyncProgress::GetAllSlaveNodes() {
  slash::RWLock l(&rwlock_, false);
  return slaves_;
}

std::unordered_map<std::string, LogOffset> SyncProgress::GetAllMatchIndex() {
  slash::RWLock l(&rwlock_, false);
  return match_index_;
}

Status SyncProgress::AddSlaveNode(const std::string& ip, int port,
    const std::string& table_name, uint32_t partition_id, int session_id) {
  std::string slave_key = ip + std::to_string(port);
  std::shared_ptr<SlaveNode> exist_ptr = GetSlaveNode(ip, port);
  if (exist_ptr) {
    LOG(WARNING) << "SlaveNode " << exist_ptr->ToString() <<
      " already exist, set new session " << session_id;
    exist_ptr->SetSessionId(session_id);
    return Status::OK();
  }
  std::shared_ptr<SlaveNode> slave_ptr =
    std::make_shared<SlaveNode>(ip, port, table_name, partition_id, session_id);
  slave_ptr->SetLastSendTime(slash::NowMicros());
  slave_ptr->SetLastRecvTime(slash::NowMicros());

  {
    slash::RWLock l(&rwlock_, true);
    slaves_[slave_key] = slave_ptr;
    // add slave to match_index
    match_index_[slave_key] = LogOffset();
  }
  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  {
    slash::RWLock l(&rwlock_, true);
    slaves_.erase(slave_key);
    // remove slave to match_index
    match_index_.erase(slave_key);
  }
  return Status::OK();
}

Status SyncProgress::Update(const std::string& ip, int port, const LogOffset& start,
      const LogOffset& end, LogOffset* committed_index) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  LogOffset acked_offset;
  {
    // update slave_ptr
    slash::MutexLock l(&slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end, &acked_offset);
    if (!s.ok()) {
      return s;
    }
    // update match_index_
    // shared slave_ptr->slave_mu
    match_index_[ip+std::to_string(port)] = acked_offset;
  }

  *committed_index = InternalCalCommittedIndex(GetAllMatchIndex());
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  slash::RWLock l(&rwlock_, false);
  return slaves_.size();
}

LogOffset SyncProgress::InternalCalCommittedIndex(std::unordered_map<std::string, LogOffset> match_index) {
  int consistency_level = g_pika_conf->consistency_level();
  std::vector<LogOffset> offsets;
  for (auto index : match_index) {
    offsets.push_back(index.second);
  }
  std::sort(offsets.begin(), offsets.end());
  LogOffset offset = offsets[offsets.size() - consistency_level];
  return offset;
}

/* MemLog */

MemLog::MemLog() {
}

int MemLog::Size() {
  return static_cast<int>(logs_.size());
}

Status MemLog::PurdgeLogs(const LogOffset& offset, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int index = InternalFindLogIndex(offset);
  if (index < 0) {
    return Status::Corruption("Cant find correct index");
  }
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return Status::OK();
}

Status MemLog::GetRangeLogs(int start, int end, std::vector<LogItem>* logs) {
  slash::MutexLock l_logs(&logs_mu_);
  int log_size = static_cast<int>(logs_.size());
  if (start > end || start >= log_size || end >= log_size) {
    return Status::Corruption("Invalid index");
  }
  logs->assign(logs_.begin() + start, logs_.begin() + end + 1);
  return Status::OK();
}

int MemLog::InternalFindLogIndex(const LogOffset& offset) {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset > offset) {
      return -1;
    }
    if (logs_[i].offset == offset) {
      return i;
    }
  }
  return -1;
}

/* ConsistencyCoordinator */

ConsistencyCoordinator::ConsistencyCoordinator(const std::string& table_name, uint32_t partition_id) : table_name_(table_name), partition_id_(partition_id) {
  std::string table_log_path = g_pika_conf->log_path() + "log_" + table_name + "/";
  std::string log_path = g_pika_conf->classic_mode() ?
    table_log_path : table_log_path + std::to_string(partition_id) + "/";
  stable_logger_ = std::make_shared<StableLog>(table_name, partition_id, log_path);
  mem_logger_ = std::make_shared<MemLog>();
  pthread_rwlock_init(&term_rwlock_, NULL);
}

ConsistencyCoordinator::~ConsistencyCoordinator() {
  pthread_rwlock_destroy(&term_rwlock_);
}

Status ConsistencyCoordinator::ProposeLog(
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  LogOffset log_offset;

  // make sure stable log and mem log consistent
  stable_logger_->Logger()->Lock();
  Status s = InternalAppendLog(cmd_ptr, &log_offset);
  if (!s.ok()) {
    return s;
  }
  if (g_pika_conf->consistency_level() == 0) {
    stable_logger_->Logger()->Unlock();
    return Status::OK();
  }
  mem_logger_->PushLog(MemLog::LogItem(log_offset, cmd_ptr, conn_ptr, resp_ptr));
  stable_logger_->Logger()->Unlock();

  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ConsistencyCoordinator::UpdateSlave(const std::string& ip, int port,
      const LogOffset& start, const LogOffset& end) {
  LogOffset committed_index;
  Status s = sync_pros_.Update(ip, port, start, end, &committed_index);
  if (!s.ok()) {
    return s;
  }

  if (g_pika_conf->consistency_level() == 0) {
    return Status::OK();
  }

  LogOffset updated_committed_index;
  bool need_update = false;
  {
    slash::MutexLock l(&index_mu_);
    need_update = InternalUpdateCommittedIndex(committed_index, &updated_committed_index);
  }
  if (need_update) {
    s = ScheduleApplyLog(updated_committed_index);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ConsistencyCoordinator::InternalUpdateCommittedIndex(const LogOffset& slave_committed_index, LogOffset* updated_committed_index) {
  if (slave_committed_index < committed_index_ ||
      slave_committed_index == committed_index_) {
    return false;
  }
  committed_index_ = slave_committed_index;
  *updated_committed_index = slave_committed_index;
  return true;
}

Status ConsistencyCoordinator::InternalAppendLog(
    std::shared_ptr<Cmd> cmd_ptr, LogOffset* log_offset) {
  uint32_t filenum = 0;
  uint64_t offset = 0;
  uint64_t logic_id = 0;

  slash::RWLock l(&term_rwlock_, false);
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &logic_id);
  uint32_t exec_time = time(nullptr);
  std::string binlog = cmd_ptr->ToBinlog(exec_time,
                                    term_,
                                    logic_id,
                                    filenum,
                                    offset);
  Status s = stable_logger_->Logger()->Put(binlog);
  if (!s.ok()) {
    return s;
  }
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
  *log_offset = LogOffset(BinlogOffset(filenum, offset), LogicOffset(term_, logic_id));
  return Status::OK();
}

Status ConsistencyCoordinator::ScheduleApplyLog(const LogOffset& committed_index) {
  std::vector<MemLog::LogItem> logs;
  Status s = mem_logger_->PurdgeLogs(committed_index, &logs);
  if (!s.ok()) {
    return Status::NotFound("committed index not found in log");
  }
  for (auto log : logs) {
    InternalApply(log);
  }
  return Status::OK();
}

Status ConsistencyCoordinator::CheckEnoughFollower() {
  if (!MatchConsistencyLevel()) {
    return Status::Incomplete("Not enough follower");
  }
  return Status::OK();
}

Status ConsistencyCoordinator::AddSlaveNode(const std::string& ip, int port, int session_id) {
  Status s = sync_pros_.AddSlaveNode(ip, port, table_name_, partition_id_, session_id);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status ConsistencyCoordinator::RemoveSlaveNode(const std::string& ip, int port) {
  Status s = sync_pros_.RemoveSlaveNode(ip, port);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

void ConsistencyCoordinator::UpdateTerm(uint32_t term) {
  slash::RWLock l(&term_rwlock_, true);
  term_ = term;
  stable_logger_->Logger()->SetTerm(term);
}

bool ConsistencyCoordinator::MatchConsistencyLevel() {
  return sync_pros_.SlaveSize() >= static_cast<int>(g_pika_conf->consistency_level());
}

void ConsistencyCoordinator::InternalApply(const MemLog::LogItem& log) {
  PikaClientConn::BgTaskArg* arg = new PikaClientConn::BgTaskArg();
  arg->cmd_ptr = log.cmd_ptr;
  arg->conn_ptr = log.conn_ptr;
  arg->resp_ptr = log.resp_ptr;
  g_pika_server->ScheduleClientBgThreads(
      PikaClientConn::DoExecTask, arg, log.cmd_ptr->current_key().front());
}

void ConsistencyCoordinator::InternalApplyStale(const MemLog::LogItem& log) {
  PikaClientConn::BgTaskArg* arg = new PikaClientConn::BgTaskArg();
  arg->cmd_ptr = log.cmd_ptr;
  arg->conn_ptr = log.conn_ptr;
  arg->resp_ptr = log.resp_ptr;
  g_pika_server->ScheduleClientBgThreads(
      PikaClientConn::DoStaleTask, arg, log.cmd_ptr->current_key().front());
}
