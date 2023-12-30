// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_LIST_H_
#define PIKA_LIST_H_

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"
#include "storage/storage.h"

/*
 * list
 */
class LIndexCmd : public Cmd {
 public:
  LIndexCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LIndexCmd(*this); }

 private:
  std::string key_;
  int64_t index_ = 0;
  void DoInitial() override;
  void Clear() override { index_ = 0; }
  rocksdb::Status s_;
};

class LInsertCmd : public Cmd {
 public:
  LInsertCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LInsertCmd(*this); }

 private:
  std::string key_;
  storage::BeforeOrAfter dir_{storage::After};
  std::string pivot_;
  std::string value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class LLenCmd : public Cmd {
 public:
  LLenCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LLenCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class BlockingBaseCmd : public Cmd {
 public:
  BlockingBaseCmd(const std::string& name, int arity, uint32_t flag, uint32_t category = 0)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST) | category) {}

  // blpop/brpop used start
  struct WriteBinlogOfPopArgs {
    BlockKeyType block_type;
    std::string key;
    std::shared_ptr<Slot> slot;
    std::shared_ptr<net::NetConn> conn;
    WriteBinlogOfPopArgs() = default;
    WriteBinlogOfPopArgs(BlockKeyType block_type_, const std::string& key_, std::shared_ptr<Slot> slot_,
                         std::shared_ptr<net::NetConn> conn_)
        : block_type(block_type_), key(key_), slot(slot_), conn(conn_) {}
  };
  void BlockThisClientToWaitLRPush(BlockKeyType block_pop_type, std::vector<std::string>& keys, int64_t expire_time);
  void TryToServeBLrPopWithThisKey(const std::string& key, std::shared_ptr<Slot> slot);
  static void ServeAndUnblockConns(void* args);
  static void WriteBinlogOfPop(std::vector<WriteBinlogOfPopArgs>& pop_args);
  void removeDuplicates(std::vector<std::string>& keys_);
  // blpop/brpop used functions end
};

class BLPopCmd final : public BlockingBaseCmd {
 public:
  BLPopCmd(const std::string& name, int arity, uint32_t flag)
      : BlockingBaseCmd(name, arity, flag, static_cast<uint32_t>(AclCategory::BLOCKING)){};
  std::vector<std::string> current_key() const override { return {keys_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new BLPopCmd(*this); }
  void DoInitial() override;
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<std::string> keys_;
  int64_t expire_time_{0};
  WriteBinlogOfPopArgs binlog_args_;
  bool is_binlog_deferred_{false};
  rocksdb::Status s_;
};

class LPopCmd : public Cmd {
 public:
  LPopCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LPopCmd(*this); }

 private:
  std::string key_;
  std::int64_t count_ = 1;
  void DoInitial() override;
  rocksdb::Status s_;
};

class LPushCmd : public BlockingBaseCmd {
 public:
  LPushCmd(const std::string& name, int arity, uint32_t flag) : BlockingBaseCmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LPushCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> values_;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override { values_.clear(); }
};

class LPushxCmd : public Cmd {
 public:
  LPushxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LPushxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  rocksdb::Status s_;
  std::vector<std::string> values_;
  void DoInitial() override;
};

class LRangeCmd : public Cmd {
 public:
  LRangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LRangeCmd(*this); }

 private:
  std::string key_;
  int64_t left_ = 0;
  int64_t right_ = 0;
  rocksdb::Status s_;
  void DoInitial() override;
};

class LRemCmd : public Cmd {
 public:
  LRemCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LRemCmd(*this); }

 private:
  std::string key_;
  int64_t count_ = 0;
  std::string value_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class LSetCmd : public Cmd {
 public:
  LSetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LSetCmd(*this); }

 private:
  std::string key_;
  int64_t index_ = 0;
  rocksdb::Status s_;
  std::string value_;
  void DoInitial() override;
};

class LTrimCmd : public Cmd {
 public:
  LTrimCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new LTrimCmd(*this); }

 private:
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
  rocksdb::Status s_;
  void DoInitial() override;
};

class BRPopCmd final : public BlockingBaseCmd {
 public:
  BRPopCmd(const std::string& name, int arity, uint32_t flag)
      : BlockingBaseCmd(name, arity, flag, static_cast<uint32_t>(AclCategory::BLOCKING)){};
  std::vector<std::string> current_key() const override { return {keys_}; }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new BRPopCmd(*this); }
  void DoInitial() override;
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<std::string> keys_;
  int64_t expire_time_{0};
  WriteBinlogOfPopArgs binlog_args_;
  bool is_binlog_deferred_{false};
};

class RPopCmd : public Cmd {
 public:
  RPopCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new RPopCmd(*this); }

 private:
  std::string key_;
  std::int64_t count_ = 1;
  void DoInitial() override;
  rocksdb::Status s_;
};

class RPopLPushCmd : public BlockingBaseCmd {
 public:
  RPopLPushCmd(const std::string& name, int arity, uint32_t flag)
      : BlockingBaseCmd(name, arity, flag, static_cast<uint32_t>(AclCategory::BLOCKING)) {
    rpop_cmd_ = std::make_shared<RPopCmd>(kCmdNameRPop, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
    lpush_cmd_ = std::make_shared<LPushCmd>(kCmdNameLPush, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
  };
  RPopLPushCmd(const RPopLPushCmd& other)
      : BlockingBaseCmd(other),
        source_(other.source_),
        receiver_(other.receiver_),
        value_poped_from_source_(other.value_poped_from_source_),
        is_write_binlog_(other.is_write_binlog_) {
    rpop_cmd_ = std::make_shared<RPopCmd>(kCmdNameRPop, 2, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
    lpush_cmd_ = std::make_shared<LPushCmd>(kCmdNameLPush, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsList);
  }
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(receiver_);
    res.push_back(source_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new RPopLPushCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::string source_;
  std::string receiver_;
  std::string value_poped_from_source_;
  bool is_write_binlog_ = false;
  // used for write binlog
  std::shared_ptr<Cmd> rpop_cmd_;
  std::shared_ptr<Cmd> lpush_cmd_;
  rocksdb::Status s_;
  void DoInitial() override;
};

class RPushCmd : public BlockingBaseCmd {
 public:
  RPushCmd(const std::string& name, int arity, uint32_t flag) : BlockingBaseCmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;

  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new RPushCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> values_;
  rocksdb::Status s_;
  void DoInitial() override;
  void Clear() override { values_.clear(); }
};

class RPushxCmd : public Cmd {
 public:
  RPushxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::LIST)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new RPushxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  std::vector<std::string> values_;
  rocksdb::Status s_;
  void DoInitial() override;
};
#endif
