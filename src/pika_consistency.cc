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
  }
  {
    // add slave to match_index
    slash::MutexLock l_match(&match_mu_);
    match_index_[slave_key] = BinlogOffset();
  }
  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  {
    slash::RWLock l(&rwlock_, true);
    slaves_.erase(slave_key);
  }
  {
    // remove slave to match_index
    slash::MutexLock l_match(&match_mu_);
    match_index_.erase(slave_key);
  }
  return Status::OK();
}

Status SyncProgress::Update(const std::string& ip, int port, const BinlogOffset& start,
      const BinlogOffset& end, BinlogOffset* committed_index) {
  std::shared_ptr<SlaveNode> slave_ptr = GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  // update slave_ptr
  BinlogOffset acked_offset;
  {
    slash::MutexLock l(&slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end);
    if (!s.ok()) {
      return s;
    }
    acked_offset = slave_ptr->acked_offset;
  }

  // update match_index_
  {
    slash::MutexLock l(&match_mu_);
    match_index_[ip+std::to_string(port)] = acked_offset;
    *committed_index = InternalCalCommittedIndex();
  }
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  slash::RWLock l(&rwlock_, false);
  return slaves_.size();
}

BinlogOffset SyncProgress::InternalCalCommittedIndex() {
  int consistency_level = g_pika_conf->consistency_level();
  std::vector<BinlogOffset> offsets;
  for (auto index : match_index_) {
    offsets.push_back(index.second);
  }
  std::sort(offsets.begin(), offsets.end());
  BinlogOffset offset = offsets[offsets.size() - consistency_level];
  return offset;
}

/* MemLog */

MemLog::MemLog() {
}

int MemLog::Size() {
  return static_cast<int>(logs_.size());
}

Status MemLog::PurdgeLogs(BinlogOffset offset, std::vector<LogItem>* logs) {
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

int MemLog::InternalFindLogIndex(BinlogOffset offset) {
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
}

Status ConsistencyCoordinator::ProposeLog(
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  BinlogOffset binlog_offset;
  stable_logger_->Logger()->Lock();
  Status s = InternalPutBinlog(cmd_ptr, &binlog_offset);
  stable_logger_->Logger()->Unlock();
  if (!s.ok()) {
    return s;
  }

  if (g_pika_conf->consistency_level() == 0) {
    return Status::OK();
  }

  mem_logger_->PushLog(MemLog::LogItem(binlog_offset, cmd_ptr, conn_ptr, resp_ptr));

  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ConsistencyCoordinator::UpdateSlave(const std::string& ip, int port,
      const BinlogOffset& start, const BinlogOffset& end) {
  BinlogOffset committed_index;
  Status s = sync_pros_.Update(ip, port, start, end, &committed_index);
  if (!s.ok()) {
    return s;
  }

  if (g_pika_conf->consistency_level() == 0) {
    return Status::OK();
  }

  bool need_update = InternalUpdateCommittedIndex(committed_index);
  if (need_update) {
    s = ScheduleApplyLog();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ConsistencyCoordinator::InternalUpdateCommittedIndex(const BinlogOffset& slave_committed_index) {
  if (slave_committed_index < committed_index_ ||
      slave_committed_index == committed_index_) {
    return false;
  }
  committed_index_ = slave_committed_index;
  return true;
}

Status ConsistencyCoordinator::InternalPutBinlog(
    std::shared_ptr<Cmd> cmd_ptr, BinlogOffset* binlog_offset) {
  uint32_t filenum = 0;
  uint64_t offset = 0;
  uint64_t logic_id = 0;

  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset, &logic_id);
  uint32_t exec_time = time(nullptr);
  std::string binlog = cmd_ptr->ToBinlog(exec_time,
                                    g_pika_conf->server_id(),
                                    logic_id,
                                    filenum,
                                    offset);
  Status s = stable_logger_->Logger()->Put(binlog);
  if (!s.ok()) {
    return s;
  }
  stable_logger_->Logger()->GetProducerStatus(&filenum, &offset);
  *binlog_offset = BinlogOffset(filenum, offset);
  return Status::OK();
}

Status ConsistencyCoordinator::ScheduleApplyLog() {
  BinlogOffset committed_index;
  {
    slash::MutexLock l_index(&index_mu_);
    committed_index = committed_index_;
  }
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
  s = AddFollower(ip, port);
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

// if this added follower just match enough follower condition
// need to purdge previews stale logs
// (slave wrote binlog local not send ack back
// or slave did not recevie this binlog yet)
Status ConsistencyCoordinator::AddFollower(const std::string& ip, int port) {
  bool before = true;
  bool after = false;
  if (!MatchConsistencyLevel()) {
    before = false;
  }
  if (MatchConsistencyLevel()) {
    after = true;
  }

  int size = mem_logger_->Size();
  std::vector<MemLog::LogItem> logs;
  mem_logger_->GetRangeLogs(0, size - 1, &logs);

  if (!before && after) {
    for (auto log : logs) {
      InternalApplyStale(log);
    }
    slash::MutexLock l_index(&index_mu_);
    committed_index_ = BinlogOffset();
  }
  return Status::OK();
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
