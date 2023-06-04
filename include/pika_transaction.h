// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_TRANSACTION_H_
#define PIKA_TRANSACTION_H_

#include "include/pika_command.h"
#include "net/include/redis_conn.h"
#include "storage/storage.h"

class MultiCmd : public Cmd {
 public:
  MultiCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new MultiCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}
 private:
  void DoInitial() override;

};

class ExecCmd : public Cmd {
 public:
  ExecCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new ExecCmd(*this); }
//  void Execute() override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}
  //NOTE(leeHao): 这个命令的key无法确定，因为每个key的db可能不一样
  std::vector<std::string> current_key() const override { return {}; }
  std::vector<std::string> GetInvolvedSlots();

 private:
  void DoInitial() override;
  std::vector<std::string> keys_;
};
class DiscardCmd : public Cmd {
 public:
  DiscardCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new DiscardCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}
 private:
  void DoInitial() override;
};



class WatchCmd : public Cmd {
 public:
  WatchCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}

  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  Cmd* Clone() override { return new WatchCmd(*this); }
  void Merge() override {}
  std::vector<std::string> current_key() const override { return keys_; }
 private:
  void DoInitial() override;
  std::vector<std::string> keys_;
  std::vector<std::string> table_keys_;
};

class UnwatchCmd : public Cmd {
 public:
  UnwatchCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}

  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  Cmd* Clone() override { return new UnwatchCmd(*this); }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {}
  void Merge() override {}
 private:
  void DoInitial() override;
};

#endif  // PIKA_TRANSACTION_H_
