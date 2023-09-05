/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_table_manager.h"
#include <memory>
#include "cmd_admin.h"
#include "cmd_kv.h"

namespace pikiwidb {

CmdTableManager::CmdTableManager() {
  cmds_ = std::make_unique<CmdTable>();
  cmds_->reserve(300);
}

void CmdTableManager::InitCmdTable() {
  std::unique_lock wl(mutex_);

  // admin
  std::unique_ptr<BaseCmd> configPtr = std::make_unique<CmdConfig>(kCmdNameConfig, -2);
  cmds_->insert(std::make_pair(kCmdNameConfig, std::move(configPtr)));

  // kv
  std::unique_ptr<BaseCmd> getPtr = std::make_unique<GetCmd>(kCmdNameGet, 2);
  cmds_->insert(std::make_pair(kCmdNameGet, std::move(getPtr)));
  std::unique_ptr<BaseCmd> setPtr = std::make_unique<SetCmd>(kCmdNameSet, -3);
  cmds_->insert(std::make_pair(kCmdNameSet, std::move(setPtr)));
}

BaseCmd* CmdTableManager::GetCommand(const std::string& cmdName) {
  std::shared_lock rl(mutex_);

  auto cmd = cmds_->find(cmdName);

  if (cmd == cmds_->end()) {
    return nullptr;
  }
  return cmd->second.get();
}

bool CmdTableManager::CmdExist(const std::string& cmd) const {
  std::shared_lock rl(mutex_);
  return cmds_->find(cmd) != cmds_->end();
}

uint32_t CmdTableManager::GetCmdId() { return ++cmdId_; }

}  // namespace pikiwidb