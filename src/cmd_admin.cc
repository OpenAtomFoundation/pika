/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_admin.h"

namespace pikiwidb {

CmdConfig::CmdConfig(const std::string& name, int arity) : BaseCmd(name, arity, CmdFlagsAdmin, AclCategoryAdmin) {
  subCmd_ = {"set", "get"};
}

bool CmdConfig::HasSubCommand() const { return true; }

std::vector<std::string> CmdConfig::SubCommand() const { return subCmd_; }

int8_t CmdConfig::SubCmdIndex(const std::string& cmdName) {
  for (size_t i = 0; i < subCmd_.size(); i++) {
    if (subCmd_[i] == cmdName) {
      return i;
    }
  }
  return -1;
}

bool CmdConfig::DoInitial(pikiwidb::CmdContext& ctx) {
  ctx.subCmd_ = ctx.argv_[1];
  std::transform(ctx.argv_[1].begin(), ctx.argv_[1].end(), ctx.subCmd_.begin(), ::tolower);
  if (ctx.subCmd_ == subCmd_[0] || ctx.subCmd_ == subCmd_[1]) {
    if (ctx.argv_.size() < 3) {
      ctx.SetRes(CmdRes::kInvalidParameter, "config " + ctx.subCmd_);
      return false;
    }
  }

  return true;
}

void CmdConfig::DoCmd(pikiwidb::CmdContext& ctx) {
  if (ctx.subCmd_ == subCmd_[0]) {
    Set(ctx);
  } else if (ctx.subCmd_ == subCmd_[1]) {
    Get(ctx);
  } else {
    ctx.SetRes(CmdRes::kSyntaxErr, "config error");
  }
}

void CmdConfig::Get(CmdContext& ctx) { ctx.AppendString("config cmd in development"); }

void CmdConfig::Set(CmdContext& ctx) { ctx.AppendString("config cmd in development"); }

}  // namespace pikiwidb