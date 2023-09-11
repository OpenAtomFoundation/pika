/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "base_cmd.h"
#include "common.h"
#include "pikiwidb.h"

namespace pikiwidb {

BaseCmd::BaseCmd(std::string name, int16_t arity, uint32_t flag, uint32_t aclCategory) {
  name_ = std::move(name);
  arity_ = arity;
  flag_ = flag;
  aclCategory_ = aclCategory;
  cmdId_ = g_pikiwidb->CmdTableManager()->GetCmdId();
}

bool BaseCmd::CheckArg(size_t num) const {
  if (arity_ > 0) {
    return num == arity_;
  }
  return num >= -arity_;
}

std::vector<std::string> BaseCmd::CurrentKey(const CmdContext& context) const {
  return std::vector<std::string>{context.key_};
}

void BaseCmd::Execute(CmdContext& ctx) {
  if (!DoInitial(ctx)) {
    return;
  }
  DoCmd(ctx);
}

std::string BaseCmd::ToBinlog(uint32_t exec_time, uint32_t term_id, uint64_t logic_id, uint32_t filenum,
                              uint64_t offset) {
  return "";
}

void BaseCmd::DoBinlog() {}
bool BaseCmd::IsWrite() const { return HasFlag(CmdFlagsWrite); }
bool BaseCmd::HasFlag(uint32_t flag) const { return flag_ & flag; }
void BaseCmd::SetFlag(uint32_t flag) { flag_ |= flag; }
void BaseCmd::ResetFlag(uint32_t flag) { flag_ &= ~flag; }
bool BaseCmd::HasSubCommand() const { return false; }
std::vector<std::string> BaseCmd::SubCommand() const { return {}; }
int8_t BaseCmd::SubCmdIndex(const std::string& cmdName) { return -1; }
uint32_t BaseCmd::AclCategory() const { return aclCategory_; }
void BaseCmd::AddAclCategory(uint32_t aclCategory) { aclCategory_ |= aclCategory; }
std::string BaseCmd::Name() const { return name_; }
// CmdRes& BaseCommand::Res() { return res_; }
// void BaseCommand::SetResp(const std::shared_ptr<std::string>& resp) { resp_ = resp; }
// std::shared_ptr<std::string> BaseCommand::GetResp() { return resp_.lock(); }
uint32_t BaseCmd::GetCmdId() const { return cmdId_; }

}  // namespace pikiwidb