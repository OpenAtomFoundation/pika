// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CMD_TABLE_MANAGER_H_
#define PIKA_CMD_TABLE_MANAGER_H_

#include <shared_mutex>
#include <thread>

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_data_distribution.h"

struct CommandStatistics {
  CommandStatistics() = default;
  CommandStatistics(const CommandStatistics& other) {
    cmd_time_consuming.store(other.cmd_time_consuming.load());
    cmd_count.store(other.cmd_count.load());
  }
  std::atomic<uint64_t> cmd_count = 0;
  std::atomic<uint64_t> cmd_time_consuming = 0;
};

class PikaCmdTableManager {
  friend AclSelector;

 public:
  PikaCmdTableManager();
  virtual ~PikaCmdTableManager() = default;
  void InitCmdTable(void);
  std::shared_ptr<Cmd> GetCmd(const std::string& opt);
  uint32_t DistributeKey(const std::string& key, uint32_t slot_num);
  bool CmdExist(const std::string& cmd) const;
  CmdTable* GetCmdTable();
  uint32_t GetCmdId();

  std::vector<std::string> GetAclCategoryCmdNames(uint32_t flag);

  /*
  * Info Commandstats used
  */
  std::unordered_map<std::string, CommandStatistics>* GetCommandStatMap();

 private:
  std::shared_ptr<Cmd> NewCommand(const std::string& opt);

  void InsertCurrentThreadDistributionMap();
  bool CheckCurrentThreadDistributionMapExist(const std::thread::id& tid);

  std::unique_ptr<CmdTable> cmds_;

  uint32_t cmdId_ = 0;

  std::shared_mutex map_protector_;
  std::unordered_map<std::thread::id, std::unique_ptr<PikaDataDistribution>> thread_distribution_map_;

  /*
  * Info Commandstats used
  */
  std::unordered_map<std::string, CommandStatistics> cmdstat_map_;
};
#endif
