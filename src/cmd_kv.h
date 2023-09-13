/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#pragma once

#include "base_cmd.h"
#include "cmd_context.h"

namespace pikiwidb {

class GetCmd : public BaseCmd {
 public:
  GetCmd(const std::string &name, int arity);

 protected:
  bool DoInitial(CmdContext &ctx) override;

 private:
  void DoCmd(CmdContext &ctx) override;
};

class SetCmd : public BaseCmd {
 public:
  SetCmd(const std::string &name, int arity);

 protected:
  bool DoInitial(CmdContext &ctx) override;

 private:
  void DoCmd(CmdContext &ctx) override;
};

}  // namespace pikiwidb
