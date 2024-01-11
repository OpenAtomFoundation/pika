// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_KV_H_
#define PIKA_KV_H_

#include "storage/storage.h"

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_slot.h"

/*
 * kv
 */
class SetCmd : public Cmd {
 public:
  enum SetCondition { kNONE, kNX, kXX, kVX, kEXORPX };
  SetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new SetCmd(*this); }
 private:
  std::string key_;
  std::string value_;
  std::string target_;
  int32_t success_ = 0;
  int64_t sec_ = 0;
  bool has_ttl_ = false;
  SetCmd::SetCondition condition_{kNONE};
  void DoInitial() override;
  void Clear() override {
    sec_ = 0;
    success_ = 0;
    condition_ = kNONE;
  }
  std::string ToRedisProtocol() override;
  rocksdb::Status s_;
};

class GetCmd : public Cmd {
 public:
  GetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new GetCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int64_t sec_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class DelCmd : public Cmd {
 public:
  DelCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new DelCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<std::string> keys_;
  int64_t split_res_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class IncrCmd : public Cmd {
 public:
  IncrCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new IncrCmd(*this); }

 private:
  std::string key_;
  int64_t new_value_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class IncrbyCmd : public Cmd {
 public:
  IncrbyCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new IncrbyCmd(*this); }

 private:
  std::string key_;
  int64_t by_ = 0, new_value_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class IncrbyfloatCmd : public Cmd {
 public:
  IncrbyfloatCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new IncrbyfloatCmd(*this); }

 private:
  std::string key_, value_, new_value_;
  double by_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class DecrCmd : public Cmd {
 public:
  DecrCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new DecrCmd(*this); }

 private:
  std::string key_;
  int64_t new_value_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class DecrbyCmd : public Cmd {
 public:
  DecrbyCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new DecrbyCmd(*this); }

 private:
  std::string key_;
  int64_t by_ = 0, new_value_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class GetsetCmd : public Cmd {
 public:
  GetsetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new GetsetCmd(*this); }

 private:
  std::string key_;
  std::string new_value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class AppendCmd : public Cmd {
 public:
  AppendCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
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
  Cmd* Clone() override { return new AppendCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class MgetCmd : public Cmd {
 public:
  MgetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)){};
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new MgetCmd(*this); }

 private:
  std::vector<std::string> keys_;
  std::string value_;
  std::vector<storage::ValueStatus> split_res_;
  std::vector<storage::ValueStatus> db_value_status_array_;
  std::vector<storage::ValueStatus> cache_value_status_array_;
  int64_t ttl_ = -1;
  void DoInitial() override;
  rocksdb::Status s_;
};

class KeysCmd : public Cmd {
 public:
  KeysCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new KeysCmd(*this); }

 private:
  std::string pattern_;
  storage::DataType type_{storage::DataType::kAll};
  void DoInitial() override;
  void Clear() override { type_ = storage::DataType::kAll; }
  rocksdb::Status s_;
};

class SetnxCmd : public Cmd {
 public:
  SetnxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new SetnxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int32_t success_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
  std::string ToRedisProtocol() override;
};

class SetexCmd : public Cmd {
 public:
  SetexCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
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
  Cmd* Clone() override { return new SetexCmd(*this); }

 private:
  std::string key_;
  int64_t sec_ = 0;
  std::string value_;
  void DoInitial() override;
  rocksdb::Status s_;
  std::string ToRedisProtocol() override;
};

class PsetexCmd : public Cmd {
 public:
  PsetexCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
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
  Cmd* Clone() override { return new PsetexCmd(*this); }

 private:
  std::string key_;
  int64_t usec_ = 0;
  std::string value_;
  void DoInitial() override;
  rocksdb::Status s_;
  std::string ToRedisProtocol() override;
};

class DelvxCmd : public Cmd {
 public:
  DelvxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new DelvxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int32_t success_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class MsetCmd : public Cmd {
 public:
  MsetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }
  MsetCmd(const MsetCmd& other) : Cmd(other), kvs_(other.kvs_) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }

  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void DoUpdateCache(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    for (auto& kv : kvs_) {
      res.push_back(kv.key);
    }
    return res;
  }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new MsetCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<storage::KeyValue> kvs_;
  void DoInitial() override;
  // used for write binlog
  std::shared_ptr<SetCmd> set_cmd_;
  rocksdb::Status s_;
};

class MsetnxCmd : public Cmd {
 public:
  MsetnxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }
  MsetnxCmd(const MsetnxCmd& other)
      : Cmd(other), kvs_(other.kvs_), success_(other.success_) {
    set_cmd_ = std::make_shared<SetCmd>(kCmdNameSet, -3, kCmdFlagsWrite | kCmdFlagsSingleSlot | kCmdFlagsKv);
  }
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    for (auto& kv : kvs_) {
      res.push_back(kv.key);
    }
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new MsetnxCmd(*this); }
  void DoBinlog(const std::shared_ptr<SyncMasterSlot>& slot) override;

 private:
  std::vector<storage::KeyValue> kvs_;
  int32_t success_ = 0;
  void DoInitial() override;
  // used for write binlog
  std::shared_ptr<SetCmd> set_cmd_;
};

class GetrangeCmd : public Cmd {
 public:
  GetrangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
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
  Cmd* Clone() override { return new GetrangeCmd(*this); }

 private:
  std::string key_;
  int64_t start_ = 0;
  int64_t end_ = 0;
  std::string value_;
  int64_t sec_ = 0;
  rocksdb::Status s_;
  void DoInitial() override;
};

class SetrangeCmd : public Cmd {
 public:
  SetrangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
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
  Cmd* Clone() override { return new SetrangeCmd(*this); }

 private:
  std::string key_;
  int64_t offset_ = 0;
  std::string value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class StrlenCmd : public Cmd {
 public:
  StrlenCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::STRING)) {}
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
  Cmd* Clone() override { return new StrlenCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int64_t sec_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class ExistsCmd : public Cmd {
 public:
  ExistsCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  std::vector<std::string> current_key() const override { return keys_; }
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override;
  void Merge() override;
  Cmd* Clone() override { return new ExistsCmd(*this); }

 private:
  std::vector<std::string> keys_;
  int64_t split_res_ = 0;
  void DoInitial() override;
};

class ExpireCmd : public Cmd {
 public:
  ExpireCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
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
  Cmd* Clone() override { return new ExpireCmd(*this); }

 private:
  std::string key_;
  int64_t sec_ = 0;
  void DoInitial() override;
  std::string ToRedisProtocol() override;
  rocksdb::Status s_;
};

class PexpireCmd : public Cmd {
 public:
  PexpireCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
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
  Cmd* Clone() override { return new PexpireCmd(*this); }

 private:
  std::string key_;
  int64_t msec_ = 0;
  void DoInitial() override;
  std::string ToRedisProtocol() override;
  rocksdb::Status s_;
};

class ExpireatCmd : public Cmd {
 public:
  ExpireatCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
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
  Cmd* Clone() override { return new ExpireatCmd(*this); }

 private:
  std::string key_;
  int64_t time_stamp_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PexpireatCmd : public Cmd {
 public:
  PexpireatCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
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
  Cmd* Clone() override { return new PexpireatCmd(*this); }

 private:
  std::string key_;
  int64_t time_stamp_ms_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
  std::string ToRedisProtocol() override;
};

class TtlCmd : public Cmd {
 public:
  TtlCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new TtlCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PttlCmd : public Cmd {
 public:
  PttlCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new PttlCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PersistCmd : public Cmd {
 public:
  PersistCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
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
  Cmd* Clone() override { return new PersistCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class TypeCmd : public Cmd {
 public:
  TypeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void ReadCache(std::shared_ptr<Slot> slot = nullptr) override;
  void DoThroughDB(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override{};
  void Merge() override{};
  Cmd* Clone() override { return new TypeCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PTypeCmd : public Cmd {
 public:
  PTypeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PTypeCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class ScanCmd : public Cmd {
 public:
  ScanCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)), pattern_("*") {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ScanCmd(*this); }

 private:
  int64_t cursor_ = 0;
  std::string pattern_ = "*";
  int64_t count_ = 10;
  storage::DataType type_ = storage::DataType::kAll;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
    type_ = storage::DataType::kAll;
  }
  rocksdb::Status s_;
};

class ScanxCmd : public Cmd {
 public:
  ScanxCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)), pattern_("*") {}
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new ScanxCmd(*this); }

 private:
  storage::DataType type_;
  std::string start_key_;
  std::string pattern_ = "*";
  int64_t count_ = 10;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    count_ = 10;
  }
  rocksdb::Status s_;
};

class PKSetexAtCmd : public Cmd {
 public:
  PKSetexAtCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKSetexAtCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  int64_t time_stamp_ = 0;
  void DoInitial() override;
  void Clear() override { time_stamp_ = 0; }
  rocksdb::Status s_;
};

class PKScanRangeCmd : public Cmd {
 public:
  PKScanRangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_start_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKScanRangeCmd(*this); }

 private:
  storage::DataType type_;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_ = "*";
  int64_t limit_ = 10;
  bool string_with_value = false;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
  rocksdb::Status s_;
};

class PKRScanRangeCmd : public Cmd {
 public:
  PKRScanRangeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::KEYSPACE)), pattern_("*") {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_start_);
    return res;
  }
  void Do(std::shared_ptr<Slot> slot = nullptr) override;
  void Split(std::shared_ptr<Slot> slot, const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKRScanRangeCmd(*this); }

 private:
  storage::DataType type_ = storage::kAll;
  std::string key_start_;
  std::string key_end_;
  std::string pattern_ = "*";
  int64_t limit_ = 10;
  bool string_with_value = false;
  void DoInitial() override;
  void Clear() override {
    pattern_ = "*";
    limit_ = 10;
    string_with_value = false;
  }
  rocksdb::Status s_;
};
#endif
