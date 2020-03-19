// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cmd_table_manager.h"

#include <unistd.h>
#include <sys/syscall.h>

#include "include/pika_conf.h"
#include "slash/include/slash_mutex.h"


#define gettid() syscall(__NR_gettid)

extern PikaConf* g_pika_conf;

PikaCmdTableManager::PikaCmdTableManager() {
  pthread_rwlock_init(&map_protector_, NULL);
  cmds_ = new CmdTable();
  cmds_->reserve(300);
  InitCmdTable(cmds_);
}

PikaCmdTableManager::~PikaCmdTableManager() {
  pthread_rwlock_destroy(&map_protector_);
  for (const auto&item : thread_distribution_map_) {
    delete item.second;
  }
  DestoryCmdTable(cmds_);
  delete cmds_;
}

std::shared_ptr<Cmd> PikaCmdTableManager::GetCmd(const std::string& opt) {
  std::string internal_opt = opt;
  return NewCommand(internal_opt);
}

std::shared_ptr<Cmd> PikaCmdTableManager::NewCommand(const std::string& opt) {
  Cmd* cmd = GetCmdFromTable(opt, *cmds_);
  if (cmd) {
    return std::shared_ptr<Cmd>(cmd->Clone());
  }
  return nullptr;
}

bool PikaCmdTableManager::CheckCurrentThreadDistributionMapExist(const pid_t& tid) {
  slash::RWLock l(&map_protector_, false);
  if (thread_distribution_map_.find(tid) == thread_distribution_map_.end()) {
    return false;
  }
  return true;
}

void PikaCmdTableManager::InsertCurrentThreadDistributionMap() {
  pid_t tid = gettid();
  PikaDataDistribution* distribution = nullptr;
  if (g_pika_conf->classic_mode()) {
    distribution = new HashModulo();
  } else {
    distribution = new Crc32();
  }
  distribution->Init();
  slash::RWLock l(&map_protector_, true);
  thread_distribution_map_.insert(std::make_pair(tid, distribution));
}

uint32_t PikaCmdTableManager::DistributeKey(const std::string& key, uint32_t partition_num) {
  pid_t tid = gettid();
  PikaDataDistribution* data_dist = nullptr;
  if (!CheckCurrentThreadDistributionMapExist(tid)) {
    InsertCurrentThreadDistributionMap();
  }

  slash::RWLock l(&map_protector_, false);
  data_dist = thread_distribution_map_[tid];
  return data_dist->Distribute(key, partition_num);
}
