// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// pika ACL command
#ifndef PIKA_ACL_CMD_H
#define PIKA_ACL_CMD_H

#include "include/pika_command.h"

class PikaAclCmd : public Cmd {
 public:
  PikaAclCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PikaAclCmd(*this); }

 private:
  void DoInitial() override;
  void Clear() override {}
};

#endif  // PIKA_ACL_CMD_H
