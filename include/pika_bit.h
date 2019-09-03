// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BIT_H_
#define PIKA_BIT_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

/*
 * bitoperation
 */
class BitGetCmd : public Cmd {
 public:
  BitGetCmd(const std::string& name, int arity, uint16_t flag)
        : Cmd(name, arity, flag) {};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) override;
  virtual Cmd* Clone() override {
    return new BitGetCmd(*this);
  }
 private:
  std::string key_;
  int64_t  bit_offset_;
  virtual void Clear() {
    key_ = "";
    bit_offset_ = -1;
  }
  virtual void DoInitial() override;
};

class BitSetCmd : public Cmd {
 public:
  BitSetCmd(const std::string& name, int arity, uint16_t flag)
        : Cmd(name, arity, flag) {};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) override;
  virtual Cmd* Clone() override {
    return new BitSetCmd(*this);
  }
 private:
  std::string key_;
  int64_t  bit_offset_;
  int64_t  on_;
  virtual void Clear() {
    key_ = "";
    bit_offset_ = -1;
    on_ = -1;
  }
  virtual void DoInitial() override;
};

class BitCountCmd : public Cmd {
 public:
  BitCountCmd(const std::string& name, int arity, uint16_t flag)
        : Cmd(name, arity, flag) {};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) override;
  virtual Cmd* Clone() override {
    return new BitCountCmd(*this);
  }
 private:
  std::string key_;
  bool  count_all_;
  int64_t  start_offset_;
  int64_t  end_offset_;
  virtual void Clear() {
    key_ = "";
    count_all_ = false;
    start_offset_ = -1;
    end_offset_ = -1;
  }
  virtual void DoInitial() override;
};

class BitPosCmd : public Cmd {
 public:
  BitPosCmd(const std::string& name, int arity, uint16_t flag)
        : Cmd(name, arity, flag) {};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) override;
  virtual Cmd* Clone() override {
    return new BitPosCmd(*this);
  }
 private:
  std::string key_;
  bool  pos_all_;
  bool  endoffset_set_;
  int64_t bit_val_;
  int64_t  start_offset_;
  int64_t  end_offset_;
  virtual void Clear() {
    key_ = "";
    pos_all_ = false;
    endoffset_set_ = false;
    bit_val_ = -1;
    start_offset_ = -1;
    end_offset_ = -1;
  }
  virtual void DoInitial() override;
};

class BitOpCmd : public Cmd {
 public:
  BitOpCmd(const std::string& name, int arity, uint16_t flag)
        : Cmd(name, arity, flag) {};
  virtual void Do(std::shared_ptr<Partition> partition = nullptr) override;
  virtual Cmd* Clone() override {
    return new BitOpCmd(*this);
  }
 private:
  std::string dest_key_;
  std::vector<std::string> src_keys_;
  blackwidow::BitOpType op_;
  virtual void Clear() {
    dest_key_ = "";
    src_keys_.clear();
    op_ = blackwidow::kBitOpDefault;
  }
  virtual void DoInitial() override;
};
#endif
