// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_consistency.h"

#include "include/pika_conf.h"

extern PikaConf* g_pika_conf;

ConsistencyCoordinator::ConsistencyCoordinator() {
}

Status ConsistencyCoordinator::ProposeLog(
    const BinlogOffset& offset,
    std::shared_ptr<Cmd> cmd_ptr,
    std::shared_ptr<PikaClientConn> conn_ptr,
    std::shared_ptr<std::string> resp_ptr) {
  slash::MutexLock l_logs(&logs_mu_);
  slash::MutexLock l_index(&index_mu_);
  if (!InternalMatchConsistencyLevel()) {
    return Status::Incomplete("Not enough follower");
  }
  logs_.push_back(LogItem(offset, cmd_ptr, conn_ptr, resp_ptr));
  return Status::OK();
}

Status ConsistencyCoordinator::ScheduleApplyLog() {
  {
    slash::MutexLock l(&index_mu_);
    InternalUpdateCommittedIndex();
  }
  std::vector<LogItem> logs;
  {
    slash::MutexLock l(&logs_mu_);
    int index = InternalFindLogIndex();
    if (index < 0) {
      return Status::NotFound("committed index not found in log");
    }
    logs.assign(logs_.begin(), logs_.begin() + index + 1);
    logs_.erase(logs_.begin(), logs_.begin() + index + 1);
  }
  for (auto log : logs) {
    InternalApply(log);
  }
  return Status::OK();
}

Status ConsistencyCoordinator::MatchConsistencyLevel() {
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
  return Status::OK();
}

Status ConsistencyCoordinator::AddFollower(const std::string& ip, int port) {
  slash::MutexLock l(&index_mu_);
  std::string ip_port = ip + std::to_string(port);
  match_index_[ip_port] = BinlogOffset();
  return Status::OK();
}

Status ConsistencyCoordinator::RemoveFollower(const std::string& ip, int port) {
  slash::MutexLock l(&index_mu_);
  std::string ip_port = ip + std::to_string(port);
  match_index_.erase(ip_port);
  return Status::OK();
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
  std::vector<BinlogOffset> offsets;
  for (auto index : match_index_) {
    offsets.push_back(index.second);
  }
  std::sort(offsets.begin(), offsets.end());
  BinlogOffset offset = offsets[consistency_level - 1];
  if (offset > committed_index_) {
    committed_index_ = offset;
  }
}

bool ConsistencyCoordinator::InternalMatchConsistencyLevel() {
  return match_index_.size() >= static_cast<size_t>(g_pika_conf->consistency_level());
}

void ConsistencyCoordinator::InternalApply(const LogItem& log) {
}
