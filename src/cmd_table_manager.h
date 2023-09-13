/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "base_cmd.h"

namespace pikiwidb {

using CmdTable = std::unordered_map<std::string, std::unique_ptr<BaseCmd>>;

class CmdTableManager {
 public:
  CmdTableManager();
  ~CmdTableManager() = default;

 public:
  void InitCmdTable();
  BaseCmd* GetCommand(const std::string& cmdName);
  //  uint32_t DistributeKey(const std::string& key, uint32_t slot_num);
  bool CmdExist(const std::string& cmd) const;
  uint32_t GetCmdId();

 private:
  std::unique_ptr<CmdTable> cmds_;

  uint32_t cmdId_ = 0;

  mutable std::shared_mutex mutex_;
};

}  // namespace pikiwidb
