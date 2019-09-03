// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HYPERLOGLOG_H_
#define PIKA_HYPERLOGLOG_H_

#include "include/pika_command.h"
#include "include/pika_partition.h"

/*
 * hyperloglog
 */
class PfAddCmd : public Cmd {
 public:
  PfAddCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PfAddCmd(*this);
  }
 private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial() override;
  virtual void Clear() {
    values_.clear();
  }
};

class PfCountCmd : public Cmd {
 public:
  PfCountCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PfCountCmd(*this);
  }
 private:
  std::vector<std::string> keys_;
  virtual void DoInitial() override;
  virtual void Clear() {
    keys_.clear();
  }
};

class PfMergeCmd : public Cmd {
 public:
  PfMergeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name,  arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new PfMergeCmd(*this);
  }
 private:
  std::vector<std::string> keys_;
  virtual void DoInitial() override;
  virtual void Clear() {
    keys_.clear();
  }
};

#endif
