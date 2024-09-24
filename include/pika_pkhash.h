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
  std::string key_, field_, value_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHSetexCmd : public Cmd {
 public:
  PKHSetexCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHSetexCmd(*this); }

 private:
  std::string key_, field_, value_;
  int64_t sec_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHExistsCmd : public Cmd {
 public:
  PKHExistsCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHExistsCmd(*this); }

 private:
  std::string key_, field_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHDelCmd : public Cmd {
 public:
  PKHDelCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHDelCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> fields_;
  int32_t deleted_ = 0;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHLenCmd : public Cmd {
 public:
  PKHLenCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHLenCmd(*this); }

 private:
  std::string key_;
  bool is_force_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHStrLenCmd : public Cmd {
 public:
  PKHStrLenCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHStrLenCmd(*this); }

 private:
  std::string key_, field_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHIncrbyCmd : public Cmd {
 public:
  PKHIncrbyCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHIncrbyCmd(*this); }

 private:
  std::string key_, field_;
  int64_t by_;
  int64_t sec_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHMSetCmd : public Cmd {
 public:
  PKHMSetCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHMSetCmd(*this); }

 private:
  std::string key_;
  std::vector<storage::FieldValue> fvs_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHMGetCmd : public Cmd {
 public:
  PKHMGetCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHMGetCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> fields_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHKeysCmd : public Cmd {
 public:
  PKHKeysCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHKeysCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHValsCmd : public Cmd {
 public:
  PKHValsCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHValsCmd(*this); }

 private:
  std::string key_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHGetAllCmd : public Cmd {
 public:
  PKHGetAllCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHGetAllCmd(*this); }

 private:
  std::string key_;
  bool is_wt_;
  void DoInitial() override;
  rocksdb::Status s_;
};

class PKHScanCmd : public Cmd {
 public:
  PKHScanCmd(const std::string& name, int arity, uint32_t flag)
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
  Cmd* Clone() override { return new PKHScanCmd(*this); }

 private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  bool is_wt_;
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
    is_wt_ = false;
  }

  void DoInitial() override;
  rocksdb::Status s_;
};

#endif
