// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cmd_table_manager.h"

#include <sys/syscall.h>
#include <unistd.h>

#include "include/pika_conf.h"
#include "pstd/include/pstd_mutex.h"

extern PikaConf* g_pika_conf;

PikaCmdTableManager::PikaCmdTableManager() {
  cmds_ = new CmdTable();
  cmds_->reserve(300);
  InitCmdTable(cmds_);
}

PikaCmdTableManager::~PikaCmdTableManager() {
  for (const auto& item : thread_distribution_map_) {
    delete item.second;
  }
  DestoryCmdTable(cmds_);
  delete cmds_;
}

std::shared_ptr<Cmd> PikaCmdTableManager::GetCmd(const std::string& opt) {
  const std::string& internal_opt = opt;
  return NewCommand(internal_opt);
}

std::shared_ptr<Cmd> PikaCmdTableManager::NewCommand(const std::string& opt) {
  Cmd* cmd = GetCmdFromTable(opt, *cmds_);
  if (cmd != nullptr) {
    return std::shared_ptr<Cmd>(cmd->Clone());
  }
  return nullptr;
}

bool PikaCmdTableManager::CheckCurrentThreadDistributionMapExist(const std::thread::id& tid) {
  std::shared_lock l(map_protector_);
  return thread_distribution_map_.find(tid) != thread_distribution_map_.end();
}

void PikaCmdTableManager::InsertCurrentThreadDistributionMap() {
  auto tid = std::this_thread::get_id();
  PikaDataDistribution* distribution = nullptr;
  distribution = new HashModulo();
  distribution->Init();
  std::lock_guard l(map_protector_);
  thread_distribution_map_.emplace(tid, distribution);
}

uint32_t PikaCmdTableManager::DistributeKey(const std::string& key, uint32_t partition_num) {
  auto tid = std::this_thread::get_id();
  PikaDataDistribution* data_dist = nullptr;
  if (!CheckCurrentThreadDistributionMapExist(tid)) {
    InsertCurrentThreadDistributionMap();
  }

  std::shared_lock l(map_protector_);
  data_dist = thread_distribution_map_[tid];
  return data_dist->Distribute(key, partition_num);
}
