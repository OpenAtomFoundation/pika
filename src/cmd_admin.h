/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#ifndef PIKIWIDB_SRC_CMD_ADMIN_H
#define PIKIWIDB_SRC_CMD_ADMIN_H

#include "base_cmd.h"
#include "cmd_context.h"

namespace pikiwidb {

class CmdConfig : public BaseCmd {
 public:
  CmdConfig(const std::string& name, int arity);

  bool HasSubCommand() const override;
  std::vector<std::string> SubCommand() const override;
  int8_t SubCmdIndex(const std::string& cmdName) override;

 protected:
  bool DoInitial(CmdContext& ctx) override;

 private:
  std::vector<std::string> subCmd_;

  void DoCmd(CmdContext& ctx) override;

  void Get(CmdContext& ctx);
  void Set(CmdContext& ctx);
};

}  // namespace pikiwidb
#endif  // PIKIWIDB_SRC_CMD_ADMIN_H
