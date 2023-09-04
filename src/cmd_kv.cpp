/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "cmd_kv.h"
#include "store.h"

namespace pikiwidb {

GetCmd::GetCmd(const std::string& name, int arity)
    : BaseCmd(name, arity, CmdFlagsReadonly, AclCategoryRead | AclCategoryString) {}

bool GetCmd::DoInitial(CmdContext& ctx) {
  ctx.key_ = ctx.argv_[1];
  return true;
}

void GetCmd::DoCmd(CmdContext& ctx) {
  PObject* value;
  PError err = PSTORE.GetValueByType(ctx.key_, value, PType_string);
  if (err != PError_ok) {
    if (err == PError_notExist) {
      ctx.AppendString("");
    } else {
      ctx.SetRes(CmdRes::kSyntaxErr, "get key error");
    }
    return;
  }
  auto str = GetDecodedString(value);
  std::string reply(str->c_str(), str->size());
  ctx.AppendString(reply);
}

SetCmd::SetCmd(const std::string& name, int arity)
    : BaseCmd(name, arity, CmdFlagsWrite, AclCategoryWrite | AclCategoryString) {}

bool SetCmd::DoInitial(CmdContext& ctx) {
  ctx.key_ = ctx.argv_[1];
  return true;
}

void SetCmd::DoCmd(CmdContext& ctx) {
  PSTORE.ClearExpire(ctx.argv_[1]);  // clear key's old ttl
  PSTORE.SetValue(ctx.argv_[1], PObject::CreateString(ctx.argv_[2]));
  ctx.SetRes(CmdRes::kOk);
}

}  // namespace pikiwidb