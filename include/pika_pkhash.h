// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PKHASH_H_
#define PIKA_PKHASH_H_

#include "include/acl.h"
#include "include/pika_command.h"
#include "include/pika_db.h"
#include "storage/storage.h"

class PKHExpireCmd : public Cmd {
 public:
  PKHExpireCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHExpireCmd(*this); }

 private:
  std::string key_;
  int64_t ttl_ = 0;
  int64_t numfields_ = 0;
  std::vector<std::string> fields_;

  rocksdb::Status s_;

  void DoInitial() override;
  void Clear() override {}
};

class PKHExpireatCmd : public Cmd {
 public:
  PKHExpireatCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHExpireatCmd(*this); }

 private:
  std::string key_;
  int64_t timestamp_ = 0;
  int64_t numfields_ = 0;
  std::vector<std::string> fields_;

  rocksdb::Status s_;

  void DoInitial() override;
  void Clear() override {}
};
class PKHExpiretimeCmd : public Cmd {
 public:
  PKHExpiretimeCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHExpiretimeCmd(*this); }

 private:
  std::string key_;
  int64_t ttl_ = 0;
  int64_t numfields_ = 0;
  std::vector<std::string> fields_;

  rocksdb::Status s_;

  void DoInitial() override;
  void Clear() override {}
};

class PKHPersistCmd : public Cmd {
 public:
  PKHPersistCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHPersistCmd(*this); }

 private:
  std::string key_;
  int64_t ttl_ = 0;
  int64_t numfields_ = 0;
  std::vector<std::string> fields_;

  rocksdb::Status s_;

  void DoInitial() override;
  void Clear() override {}
};

class PKHTTLCmd : public Cmd {
 public:
  PKHTTLCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHTTLCmd(*this); }

 private:
  std::string key_;
  int64_t ttl_ = 0;
  int64_t numfields_ = 0;
  std::vector<std::string> fields_;

  rocksdb::Status s_;

  void DoInitial() override;
  void Clear() override {}
};

class PKHGetCmd : public Cmd {
 public:
  PKHGetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void ReadCache() override;
  void DoThroughDB() override;
  void DoUpdateCache() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHGetCmd(*this); }

 private:
  std::string key_, field_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHSetCmd : public Cmd {
 public:
  PKHSetCmd(const std::string& name, int arity, uint32_t flag)
      : Cmd(name, arity, flag, static_cast<uint32_t>(AclCategory::PKHASH)) {}
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  void Do() override;
  void DoThroughDB() override;
  void DoUpdateCache() override;
  void Split(const HintKeys& hint_keys) override {};
  void Merge() override {};
  Cmd* Clone() override { return new PKHSetCmd(*this); }

 private:
  // 每个命令的参数组成不同。
  std::string key_, field_, value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

#endif
