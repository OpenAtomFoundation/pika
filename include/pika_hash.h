// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HASH_H_
#define PIKA_HASH_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

/*
 * hash
 */
class HDelCmd : public Cmd {
 public:
  HDelCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HDelCmd(*this);
  }
 private:
  std::string key_;
  std::vector<std::string> fields_;
  virtual void DoInitial() override;
};

class HGetCmd : public Cmd {
 public:
  HGetCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HGetCmd(*this);
  }
 private:
  std::string key_, field_;
  virtual void DoInitial() override;
};

class HGetallCmd : public Cmd {
 public:
  HGetallCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HGetallCmd(*this);
  }
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class HSetCmd : public Cmd {
 public:
  HSetCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HSetCmd(*this);
  }
 private:
  std::string key_, field_, value_;
  virtual void DoInitial() override;
};

class HExistsCmd : public Cmd {
 public:
  HExistsCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HExistsCmd(*this);
  }
 private:
  std::string key_, field_;
  virtual void DoInitial() override;
};

class HIncrbyCmd : public Cmd {
 public:
  HIncrbyCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HIncrbyCmd(*this);
  }
 private:
  std::string key_, field_;
  int64_t by_;
  virtual void DoInitial() override;
};

class HIncrbyfloatCmd : public Cmd {
 public:
  HIncrbyfloatCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HIncrbyfloatCmd(*this);
  }
 private:
  std::string key_, field_, by_;
  virtual void DoInitial() override;
};

class HKeysCmd : public Cmd {
 public:
  HKeysCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HKeysCmd(*this);
  }
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class HLenCmd : public Cmd {
 public:
  HLenCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HLenCmd(*this);
  }
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class HMgetCmd : public Cmd {
 public:
  HMgetCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HMgetCmd(*this);
  }
 private:
  std::string key_;
  std::vector<std::string> fields_;
  virtual void DoInitial() override;
};

class HMsetCmd : public Cmd {
 public:
  HMsetCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HMsetCmd(*this);
  }
 private:
  std::string key_;
  std::vector<blackwidow::FieldValue> fvs_;
  virtual void DoInitial() override;
};

class HSetnxCmd : public Cmd {
 public:
  HSetnxCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HSetnxCmd(*this);
  }
 private:
  std::string key_, field_, value_;
  virtual void DoInitial() override;
};

class HStrlenCmd : public Cmd {
 public:
  HStrlenCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HStrlenCmd(*this);
  }
 private:
  std::string key_, field_;
  virtual void DoInitial() override;
};

class HValsCmd : public Cmd {
 public:
  HValsCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HValsCmd(*this);
  }
 private:
  std::string key_, field_;
  virtual void DoInitial() override;
};

class HScanCmd : public Cmd {
 public:
  HScanCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*"), count_(10) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HScanCmd(*this);
  }
 private:
  std::string key_, pattern_;
  int64_t cursor_, count_;
  virtual void DoInitial() override;
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class HScanxCmd : public Cmd {
 public:
  HScanxCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*"), count_(10) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new HScanxCmd(*this);
  }
 private:
  std::string key_, start_field_, pattern_;
  int64_t count_;
  virtual void DoInitial() override;
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class PKHScanRangeCmd : public Cmd {
 public:
  PKHScanRangeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*"), limit_(10) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PKHScanRangeCmd(*this);
  }
 private:
  std::string key_;
  std::string field_start_;
  std::string field_end_;
  std::string pattern_;
  int64_t limit_;
  virtual void DoInitial() override;
  virtual void Clear() {
    pattern_ = "*";
    limit_ = 10;
  }
};

class PKHRScanRangeCmd : public Cmd {
 public:
  PKHRScanRangeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*"), limit_(10) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PKHRScanRangeCmd(*this);
  }
 private:
  std::string key_;
  std::string field_start_;
  std::string field_end_;
  std::string pattern_;
  int64_t limit_;
  virtual void DoInitial() override;
  virtual void Clear() {
    pattern_ = "*";
    limit_ = 10;
  }
};
#endif
