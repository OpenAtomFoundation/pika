// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SET_H_
#define PIKA_SET_H_

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "pika_kv.h"

/*
 * set
 */
class SAddCmd : public Cmd {
 public:
  SAddCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SAddCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SPopCmd : public Cmd {
 public:
  SPopCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SPopCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  int64_t count_ = 1;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SCardCmd : public Cmd {
 public:
  SCardCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SCardCmd(*this); }

 private:
  std::string key_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SMembersCmd : public Cmd {
 public:
  SMembersCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SMembersCmd(*this); }

 private:
  std::string key_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SScanCmd : public Cmd {
 public:
  SScanCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SScanCmd(*this); }

 private:
  std::string key_, pattern_ = "*";
  int64_t cursor_ = 0;
  int64_t count_ = 10;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
};

class SRemCmd : public Cmd {
 public:
  SRemCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SRemCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> members_;
  rocksdb::Status s_;
  int32_t deleted_ = 0;
  void DoInitial() override;
};

class SUnionCmd : public Cmd {
 public:
  SUnionCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SUnionCmd(*this); }

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SetOperationCmd : public Cmd {
 public:
  SetOperationCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {
    sadd_cmd_ = std::make_shared<SAddCmd>(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
    del_cmd_ = std::make_shared<DelCmd>(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv);
  }
  SetOperationCmd(const SetOperationCmd& other)
      : Cmd(other), dest_key_(other.dest_key_), value_to_dest_(other.value_to_dest_) {
    sadd_cmd_ = std::make_shared<SAddCmd>(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
    del_cmd_ = std::make_shared<DelCmd>(kCmdNameDel, -2, kCmdFlagsWrite | kCmdFlagsMultiSlot | kCmdFlagsKv);
  }

  std::vector<std::string> current_key() const override { return {dest_key_}; }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 protected:
  std::string dest_key_;
  std::vector<std::string> keys_;
  // used for write binlog
  std::shared_ptr<Cmd> sadd_cmd_;
  std::shared_ptr<Cmd> del_cmd_;
  std::vector<std::string> value_to_dest_;
};

class SUnionstoreCmd : public SetOperationCmd {
 public:
  SUnionstoreCmd(const std::string& name, int arity, uint32_t flag) : SetOperationCmd(name, arity, flag) {}
  // current_key() is override in base class : SetOperationCmd
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SUnionstoreCmd(*this); }

 private:
  void DoInitial() override;
  rocksdb::Status s_;
};

class SInterCmd : public Cmd {
 public:
  SInterCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SInterCmd(*this); }

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SInterstoreCmd : public SetOperationCmd {
 public:
  SInterstoreCmd(const std::string& name, int arity, uint32_t flag) : SetOperationCmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SInterstoreCmd(*this); }

 private:
  void DoInitial() override;
  rocksdb::Status s_;
};

class SIsmemberCmd : public Cmd {
 public:
  SIsmemberCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SIsmemberCmd(*this); }

 private:
   std::string key_;
   std::string member_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SDiffCmd : public Cmd {
 public:
  SDiffCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SDiffCmd(*this); }

 private:
  std::vector<std::string> keys_;
  void DoInitial() override;
};

class SDiffstoreCmd : public SetOperationCmd {
 public:
  SDiffstoreCmd(const std::string& name, int arity, uint32_t flag) : SetOperationCmd(name, arity, flag) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SDiffstoreCmd(*this); }

 private:
  rocksdb::Status s_;
  void DoInitial() override;
};

class SMoveCmd : public Cmd {
 public:
  SMoveCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {
    srem_cmd_ = std::make_shared<SRemCmd>(kCmdNameSRem, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
    sadd_cmd_ = std::make_shared<SAddCmd>(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
  }
  SMoveCmd(const SMoveCmd& other)
      : Cmd(other),
        src_key_(other.src_key_),
        dest_key_(other.dest_key_),
        member_(other.member_),
        move_success_(other.move_success_) {
    srem_cmd_ = std::make_shared<SRemCmd>(kCmdNameSRem, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
    sadd_cmd_ = std::make_shared<SAddCmd>(kCmdNameSAdd, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsSet);
  }
  std::vector<std::string> current_key() const override { return {src_key_, dest_key_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SMoveCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::string src_key_, dest_key_, member_;
  void DoInitial() override;
  // used for write binlog
  std::shared_ptr<SRemCmd> srem_cmd_;
  std::shared_ptr<SAddCmd> sadd_cmd_;
  int32_t move_success_{0};
};

class SRandmemberCmd : public Cmd {
 public:
  SRandmemberCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::SET)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new SRandmemberCmd(*this); }

 private:
  std::string key_;
  int64_t count_ = 1;
  bool reply_arr = false;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override {
    count_ = 1;
    reply_arr = false;
  }
};

#endif
