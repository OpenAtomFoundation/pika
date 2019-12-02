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
  slash::RWLock l(&rwlock_, true);
  std::shared_ptr<SlaveNode> slave_ptr =
    std::make_shared<SlaveNode>(ip, port, table_name, partition_id, session_id);
  slave_ptr->SetLastSendTime(slash::NowMicros());
  slave_ptr->SetLastRecvTime(slash::NowMicros());
  slaves_[slave_key] = slave_ptr;
  return Status::OK();
}

Status SyncProgress::RemoveSlaveNode(const std::string& ip, int port) {
  std::string slave_key = ip + std::to_string(port);
  slash::RWLock l(&rwlock_, true);
  slaves_.erase(slave_key);
  return Status::OK();
}

int SyncProgress::SlaveSize() {
  slash::RWLock l(&rwlock_, false);
  return slaves_.size();
}

/* ConsistencyCoordinator */

ConsistencyCoordinator::ConsistencyCoordinator(const std::string& table_name, uint32_t partition_id) : table_name_(table_name), partition_id_(partition_id) {
  std::string table_log_path = g_pika_conf->log_path() + "log_" + table_name + "/";
  std::string log_path = g_pika_conf->classic_mode() ?
    table_log_path : table_log_path + std::to_string(partition_id) + "/";
  stable_logger_ = std::make_shared<StableLog>(table_name, partition_id, log_path);
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

  {
    slash::MutexLock l_logs(&logs_mu_);
    logs_.push_back(LogItem(binlog_offset, cmd_ptr, conn_ptr, resp_ptr));
  }

  g_pika_server->SignalAuxiliary();
  return Status::OK();
}

Status ConsistencyCoordinator::UpdateSlave(const std::string& ip, int port,
      const BinlogOffset& start, const BinlogOffset& end) {
  std::shared_ptr<SlaveNode> slave_ptr = sync_pros_.GetSlaveNode(ip, port);
  if (!slave_ptr) {
    return Status::NotFound("ip " + ip  + " port " + std::to_string(port));
  }

  BinlogOffset acked_offset;
  {
    slash::MutexLock l(&slave_ptr->slave_mu);
    Status s = slave_ptr->Update(start, end);
    if (!s.ok()) {
      return s;
    }
    acked_offset = slave_ptr->acked_offset;
  }

  if (g_pika_conf->consistency_level() == 0) {
    return Status::OK();
  }

  Status s = UpdateMatchIndex(ip, port, acked_offset);
  if (!s.ok()) {
    return s;
  }
  s = ScheduleApplyLog();
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

Status ConsistencyCoordinator::UpdateMatchIndex(
    const std::string& ip, int port, const BinlogOffset& offset) {
  slash::MutexLock l(&index_mu_);
  std::string ip_port = ip + std::to_string(port);
  match_index_[ip_port] = offset;
  InternalUpdateCommittedIndex();
  return Status::OK();
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
  std::vector<LogItem> logs;
  int res = 0;
  {
    slash::MutexLock l(&logs_mu_);
    InternalPurdgeLog(&logs);
  }
  if (res != 0) {
    return Status::NotFound("committed index not found in log");
  }
  for (auto log : logs) {
    InternalApply(log);
  }
  return Status::OK();
}

Status ConsistencyCoordinator::CheckEnoughFollower() {
  slash::MutexLock l(&index_mu_);
  if (!InternalMatchConsistencyLevel()) {
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
  s = RemoveFollower(ip, port);
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
  slash::MutexLock l_logs(&logs_mu_);
  slash::MutexLock l_index(&index_mu_);
  if (!InternalMatchConsistencyLevel()) {
    before = false;
  }
  std::string ip_port = ip + std::to_string(port);
  match_index_[ip_port] = BinlogOffset();
  if (InternalMatchConsistencyLevel()) {
    after = true;
  }
  if (!before && after) {
    for (auto log : logs_) {
      InternalApplyStale(log);
    }
    committed_index_ = BinlogOffset();
  }
  return Status::OK();
}

Status ConsistencyCoordinator::RemoveFollower(const std::string& ip, int port) {
  slash::MutexLock l(&index_mu_);
  std::string ip_port = ip + std::to_string(port);
  match_index_.erase(ip_port);
  return Status::OK();
}

size_t ConsistencyCoordinator::LogsSize() {
  slash::MutexLock l(&logs_mu_);
  return logs_.size();
}

int ConsistencyCoordinator::InternalFindLogIndex() {
  for (size_t i = 0; i < logs_.size(); ++i) {
    if (logs_[i].offset > committed_index_) {
      return -1;
    }
    if (logs_[i].offset == committed_index_) {
      return i;
    }
  }
  return -1;
}

void ConsistencyCoordinator::InternalUpdateCommittedIndex() {
  int consistency_level = g_pika_conf->consistency_level();
  if (!InternalMatchConsistencyLevel()) {
    return;
  }
  std::vector<BinlogOffset> offsets;
  for (auto index : match_index_) {
    offsets.push_back(index.second);
  }
  std::sort(offsets.begin(), offsets.end());
  BinlogOffset offset = offsets[offsets.size() - consistency_level];
  if (offset > committed_index_) {
    committed_index_ = offset;
  }
}

bool ConsistencyCoordinator::InternalMatchConsistencyLevel() {
  return match_index_.size() >= static_cast<size_t>(g_pika_conf->consistency_level());
}

int ConsistencyCoordinator::InternalPurdgeLog(std::vector<LogItem>* logs) {
  int index = InternalFindLogIndex();
  if (index < 0) {
    return -1;
  }
  logs->assign(logs_.begin(), logs_.begin() + index + 1);
  logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  return 0;
}

void ConsistencyCoordinator::InternalApply(const LogItem& log) {
  PikaClientConn::BgTaskArg* arg = new PikaClientConn::BgTaskArg();
  arg->cmd_ptr = log.cmd_ptr;
  arg->conn_ptr = log.conn_ptr;
  arg->resp_ptr = log.resp_ptr;
  g_pika_server->ScheduleClientBgThreads(
      PikaClientConn::DoExecTask, arg, log.cmd_ptr->current_key().front());
}

void ConsistencyCoordinator::InternalApplyStale(const LogItem& log) {
  PikaClientConn::BgTaskArg* arg = new PikaClientConn::BgTaskArg();
  arg->cmd_ptr = log.cmd_ptr;
  arg->conn_ptr = log.conn_ptr;
  arg->resp_ptr = log.resp_ptr;
  g_pika_server->ScheduleClientBgThreads(
      PikaClientConn::DoStaleTask, arg, log.cmd_ptr->current_key().front());
}
