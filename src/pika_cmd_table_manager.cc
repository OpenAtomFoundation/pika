// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cmd_table_manager.h"

#include <sys/syscall.h>
#include <unistd.h>

#include "include/acl.h"
#include "include/pika_conf.h"
#include "pstd/include/pstd_mutex.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

PikaCmdTableManager::PikaCmdTableManager() {
  cmds_ = std::make_unique<CmdTable>();
  cmds_->reserve(300);
}

void PikaCmdTableManager::InitCmdTable(void) {
  ::InitCmdTable(cmds_.get());
  
  for (const auto& cmd : *cmds_) {
    if (cmd.second->flag() & kCmdFlagsWrite) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::WRITE));
    }
    if (cmd.second->flag() & kCmdFlagsRead &&
        !(cmd.second->AclCategory() & static_cast<uint32_t>(AclCategory::SCRIPTING))) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::READ));
    }
    if (cmd.second->flag() & kCmdFlagsAdmin) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::ADMIN) |
                                 static_cast<uint32_t>(AclCategory::DANGEROUS));
    }
    if (cmd.second->flag() & kCmdFlagsPubSub) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::PUBSUB));
    }
    if (cmd.second->flag() & kCmdFlagsFast) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::FAST));
    }
    if (cmd.second->flag() & kCmdFlagsSlow) {
      cmd.second->AddAclCategory(static_cast<uint32_t>(AclCategory::SLOW));
    }
  }

  CommandStatistics statistics;
  for (auto& iter : *cmds_) {
    cmdstat_map_.emplace(iter.first, statistics);
  }
}

std::unordered_map<std::string, CommandStatistics>* PikaCmdTableManager::GetCommandStatMap() {
  return &cmdstat_map_;
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

CmdTable* PikaCmdTableManager::GetCmdTable() { return cmds_.get(); }

uint32_t PikaCmdTableManager::GetCmdId() { return ++cmdId_; }

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

bool PikaCmdTableManager::CmdExist(const std::string& cmd) const { return cmds_->find(cmd) != cmds_->end(); }

std::vector<std::string> PikaCmdTableManager::GetAclCategoryCmdNames(uint32_t flag) {
  std::vector<std::string> result;
  for (const auto& item : (*cmds_)) {
    if (item.second->AclCategory() & flag) {
      result.emplace_back(item.first);
    }
  }
  return result;
}
