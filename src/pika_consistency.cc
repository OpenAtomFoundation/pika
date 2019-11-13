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

ConsistencyCoordinator::ConsistencyCoordinator() {
}

// if client request CheckEnoughFollower pass
// this log will push to logs_ anyway, or it will cause log and db dismatch
Status ConsistencyCoordinator::ProposeLog(
    const BinlogOffset& offset,
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  slash::MutexLock l_logs(&logs_mu_);
  logs_.push_back(LogItem(offset, cmd_ptr, conn_ptr, resp_ptr));
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

Status ConsistencyCoordinator::UpdateMatchIndex(
    const std::string& ip, int port, const BinlogOffset& offset) {
  slash::MutexLock l(&index_mu_);
  std::string ip_port = ip + std::to_string(port);
  match_index_[ip_port] = offset;
  InternalUpdateCommittedIndex();
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
