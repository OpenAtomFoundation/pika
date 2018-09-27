// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CMD_TABLE_MANAGER_H_
#define PIKA_CMD_TABLE_MANAGER_H_
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include "slash/include/slash_mutex.h"
#include "include/pika_command.h"

#define gettid() syscall(__NR_gettid)

class PikaCmdTableManager {
 public:
  PikaCmdTableManager();
  virtual ~PikaCmdTableManager();
  Cmd* GetCmd(const std::string& opt);
 private:
  void InsertCurrentThreadCmdTable();
  bool CheckCurrentThreadCmdTableExist(const pid_t& tid);

  pthread_rwlock_t map_protector_;
  std::unordered_map<pid_t, CmdTable*> thread_table_map_;
};
#endif
