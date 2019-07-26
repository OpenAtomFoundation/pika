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
}

PikaCmdTableManager::~PikaCmdTableManager() {
  pthread_rwlock_destroy(&map_protector_);
  for (const auto& item : thread_table_map_) {
    CmdTable* cmd_table = item.second;
    CmdTable::const_iterator it = cmd_table->begin();
    for (; it != cmd_table->end(); ++it) {
      delete it->second;
    }
    delete cmd_table;
  }
}

Cmd* PikaCmdTableManager::GetCmd(const std::string& opt) {
  pid_t tid = gettid();
  CmdTable* cmd_table = nullptr;
  if (!CheckCurrentThreadCmdTableExist(tid)) {
    InsertCurrentThreadCmdTable();
  }

  slash::RWLock l(&map_protector_, false);
  cmd_table = thread_table_map_[tid];
  std::string internal_opt = opt;
  if (!g_pika_conf->classic_mode()) {
    TryChangeToAlias(&internal_opt);
  }
  CmdTable::const_iterator iter = cmd_table->find(internal_opt);
  if (iter != cmd_table->end()) {
    return iter->second;
  }
  return NULL;
}

void PikaCmdTableManager::TryChangeToAlias(std::string *internal_opt) {
  if (!strcasecmp(internal_opt->c_str(), kCmdNameSlaveof.c_str())) {
    *internal_opt = kCmdNamePkClusterSlotsSlaveof;
  }
}

bool PikaCmdTableManager::CheckCurrentThreadCmdTableExist(const pid_t& tid) {
  slash::RWLock l(&map_protector_, false);
  if (thread_table_map_.find(tid) == thread_table_map_.end()) {
    return false;
  }
  return true;
}

void PikaCmdTableManager::InsertCurrentThreadCmdTable() {
  pid_t tid = gettid();
  CmdTable* cmds = new CmdTable();
  cmds->reserve(300);
  InitCmdTable(cmds);
  slash::RWLock l(&map_protector_, true);
  thread_table_map_.insert(std::make_pair(tid, cmds));
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
