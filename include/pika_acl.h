// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// pika ACL command
#ifndef PIKA_ACL_CMD_H
#define PIKA_ACL_CMD_H

#include "include/pika_command.h"
#include "include/pika_server.h"

extern PikaServer* g_pika_server;

class PikaAclCmd : public Cmd {
 public:
  PikaAclCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::ADMIN)) {
    subCmdName_ = {"cat", "deluser", "dryrun",  "genpass", "getuser", "list", "load",
                   "log", "save",    "setuser", "users",   "whoami",  "help"};
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PikaAclCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override {}

  void Cat();
  void DelUser();
  void DryRun();
  void GenPass();
  void GetUser();
  void List();
  void Load();
  void Log();
  void Save();
  void SetUser();
  void Users();
  void WhoAmI();
  void Help();

  std::string subCmd_;
};

#endif  // PIKA_ACL_CMD_H
