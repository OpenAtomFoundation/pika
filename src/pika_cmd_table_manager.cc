// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cmd_table_manager.h"

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
  CmdTable::const_iterator iter = cmd_table->find(opt);
  if (iter != cmd_table->end()) {
    return iter->second;
  }
  return NULL;
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
  thread_table_map_.insert(make_pair(tid, cmds));
}
