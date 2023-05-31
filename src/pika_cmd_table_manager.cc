// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cmd_table_manager.h"

#include <sys/syscall.h>
#include <unistd.h>

#include "include/pika_conf.h"
#include "pstd/include/pstd_mutex.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

PikaCmdTableManager::PikaCmdTableManager() {
  cmds_ = std::make_unique<CmdTable>();
  cmds_->reserve(300);
  InitCmdTable(cmds_.get());
}

std::shared_ptr<Cmd> PikaCmdTableManager::GetCmd(const std::string& opt) {
  const std::string& internal_opt = opt;
  return NewCommand(internal_opt);
}

std::shared_ptr<Cmd> PikaCmdTableManager::NewCommand(const std::string& opt) {
  Cmd* cmd = GetCmdFromDB(opt, *cmds_);
  if (cmd) {
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
  std::unique_ptr<PikaDataDistribution> distribution = std::make_unique<HashModulo>();
  distribution->Init();
  std::lock_guard l(map_protector_);
  thread_distribution_map_.emplace(tid, std::move(distribution));
}

uint32_t PikaCmdTableManager::DistributeKey(const std::string& key, uint32_t slot_num) {
  auto tid = std::this_thread::get_id();
  if (!CheckCurrentThreadDistributionMapExist(tid)) {
    InsertCurrentThreadDistributionMap();
  }

  std::shared_lock l(map_protector_);
  return thread_distribution_map_[tid]->Distribute(key, slot_num);
}
