// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HYPERLOGLOG_H_
#define PIKA_HYPERLOGLOG_H_

#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "include/pika_kv.h"
/*
 * hyperloglog
 */
class PfAddCmd : public Cmd {
 public:
  PfAddCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PfAddCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> values_;
  void DoInitial() override;
  void Clear() override { values_.clear(); }
};

class PfCountCmd : public Cmd {
 public:
  PfCountCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PfCountCmd(*this); }

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
  void Clear() override { keys_.clear(); }
};

class PfMergeCmd : public Cmd {
 public:
  PfMergeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }
  PfMergeCmd(const PfMergeCmd& other)
      : Cmd(other), keys_(other.keys_), value_to_dest_(other.value_to_dest_) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }
  std::vector<std::string> current_key() const override {
    return keys_;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PfMergeCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
  void Clear() override { keys_.clear(); }
  // used for write binlog
  std::string value_to_dest_;
  std::shared_ptr<SetCmd> set_cmd_;
};

#endif
