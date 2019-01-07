// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_LIST_H_
#define PIKA_LIST_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"


/*
 * list
 */
class LIndexCmd : public Cmd {
 public:
  LIndexCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), index_(0) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  int64_t index_;
  virtual void DoInitial() override;
  virtual void Clear() {
    index_ = 0;
  }
};

class LInsertCmd : public Cmd {
 public:
  LInsertCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), dir_(blackwidow::After) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  blackwidow::BeforeOrAfter dir_;
  std::string pivot_;
  std::string value_;
  virtual void DoInitial() override;
};

class LLenCmd : public Cmd {
 public:
  LLenCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class LPopCmd : public Cmd {
 public:
  LPopCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class LPushCmd : public Cmd {
 public:
  LPushCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial() override;
  virtual void Clear() {
    values_.clear();
  }
};

class LPushxCmd : public Cmd {
 public:
  LPushxCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  std::string value_;
  virtual void DoInitial() override;
};

class LRangeCmd : public Cmd {
 public:
  LRangeCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_(0), right_(0) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  int64_t left_;
  int64_t right_;
  virtual void DoInitial() override;
};

class LRemCmd : public Cmd {
 public:
  LRemCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), count_(0) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  int64_t count_;
  std::string value_;
  virtual void DoInitial() override;
};

class LSetCmd : public Cmd {
 public:
  LSetCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), index_(0) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  int64_t index_;
  std::string value_;
  virtual void DoInitial() override;
};

class LTrimCmd : public Cmd {
 public:
  LTrimCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), start_(0), stop_(0) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  int64_t start_;
  int64_t stop_;
  virtual void DoInitial() override;
};

class RPopCmd : public Cmd {
 public:
  RPopCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class RPopLPushCmd : public Cmd {
 public:
  RPopLPushCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string source_;
  std::string receiver_;
  virtual void DoInitial() override;
};

class RPushCmd : public Cmd {
 public:
  RPushCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial() override;
  virtual void Clear() {
    values_.clear();
  }
};

class RPushxCmd : public Cmd {
 public:
  RPushxCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {};
  virtual std::string current_key() const { return key_; }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string key_;
  std::string value_;
  virtual void DoInitial() override;
};
#endif
