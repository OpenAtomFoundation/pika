// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_ZSET_H_
#define PIKA_ZSET_H_

#include "blackwidow/blackwidow.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"

/*
 * zset
 */
class ZAddCmd : public Cmd {
 public:
  ZAddCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZAddCmd(*this);
  }
 private:
  std::string key_;
  std::vector<blackwidow::ScoreMember> score_members;
  virtual void DoInitial() override;
};

class ZCardCmd : public Cmd {
 public:
  ZCardCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZCardCmd(*this);
  }
 private:
  std::string key_;
  virtual void DoInitial() override;
};

class ZScanCmd : public Cmd {
 public:
  ZScanCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), pattern_("*"), count_(10) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZScanCmd(*this);
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

class ZIncrbyCmd : public Cmd {
 public:
  ZIncrbyCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZIncrbyCmd(*this);
  }
 private:
  std::string key_, member_;
  double by_;
  virtual void DoInitial() override;
};

class ZsetRangeParentCmd : public Cmd {
 public:
  ZsetRangeParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), is_ws_(false) {}
 protected:
  std::string key_;
  int64_t start_, stop_;
  bool is_ws_;
  virtual void DoInitial() override;
  virtual void Clear() {
    is_ws_ = false;
  }
};

class ZRangeCmd : public ZsetRangeParentCmd {
 public:
  ZRangeCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangeParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRangeCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZRevrangeCmd : public ZsetRangeParentCmd {
 public:
  ZRevrangeCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangeParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRevrangeCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZsetRangebyscoreParentCmd : public Cmd {
 public:
  ZsetRangebyscoreParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true), with_scores_(false), offset_(0), count_(-1) {}
 protected:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_, with_scores_;
  int64_t offset_, count_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ = right_close_ = true;
    with_scores_ = false;
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebyscoreCmd : public ZsetRangebyscoreParentCmd {
 public:
  ZRangebyscoreCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangebyscoreParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRangebyscoreCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZRevrangebyscoreCmd : public ZsetRangebyscoreParentCmd {
 public:
  ZRevrangebyscoreCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangebyscoreParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRevrangebyscoreCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZCountCmd : public Cmd {
 public:
  ZCountCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZCountCmd(*this);
  }
 private:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ = true;
    right_close_ = true;
  }
};

class ZRemCmd : public Cmd {
 public:
  ZRemCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRemCmd(*this);
  }
 private:
  std::string key_;
  std::vector<std::string> members_;
  virtual void DoInitial() override;
};

class ZsetUIstoreParentCmd : public Cmd {
 public:
  ZsetUIstoreParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), aggregate_(blackwidow::SUM) {}
 protected:
  std::string dest_key_;
  int64_t num_keys_;
  blackwidow::AGGREGATE aggregate_;
  std::vector<std::string> keys_;
  std::vector<double> weights_;
  virtual void DoInitial() override;
  virtual void Clear() {
    aggregate_ = blackwidow::SUM;
  }
};

class ZUnionstoreCmd : public ZsetUIstoreParentCmd {
 public:
  ZUnionstoreCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetUIstoreParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZUnionstoreCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZInterstoreCmd : public ZsetUIstoreParentCmd {
 public:
  ZInterstoreCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetUIstoreParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZInterstoreCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZsetRankParentCmd : public Cmd {
 public:
  ZsetRankParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
 protected:
  std::string key_, member_;
  virtual void DoInitial() override;
};

class ZRankCmd : public ZsetRankParentCmd {
 public:
  ZRankCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRankParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRankCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZRevrankCmd : public ZsetRankParentCmd {
 public:
  ZRevrankCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRankParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRevrankCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class ZScoreCmd : public ZsetRankParentCmd {
 public:
  ZScoreCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRankParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZScoreCmd(*this);
  }
 private:
  std::string key_, member_;
  virtual void DoInitial() override;
};


class ZsetRangebylexParentCmd : public Cmd {
 public:
  ZsetRangebylexParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true), offset_(0), count_(-1) {}
 protected:
  std::string key_, min_member_, max_member_;
  bool left_close_, right_close_;
  int64_t offset_, count_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ = right_close_ = true;
    offset_ = 0;
    count_ = -1;
  }
};

class ZRangebylexCmd : public ZsetRangebylexParentCmd {
 public:
  ZRangebylexCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangebylexParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRangebylexCmd(*this);
  }
 private: 
  virtual void DoInitial() override;
};

class ZRevrangebylexCmd : public ZsetRangebylexParentCmd {
 public:
  ZRevrangebylexCmd(const std::string& name, int arity, uint16_t flag)
      : ZsetRangebylexParentCmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRevrangebylexCmd(*this);
  }
 private: 
  virtual void DoInitial() override;
};

class ZLexcountCmd : public Cmd {
 public:
  ZLexcountCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZLexcountCmd(*this);
  }
 private:
  std::string key_, min_member_, max_member_;
  bool left_close_, right_close_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ = right_close_ = true;
  }
};

class ZRemrangebyrankCmd : public Cmd {
 public:
  ZRemrangebyrankCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRemrangebyrankCmd(*this);
  }
 private:
  std::string key_;
  int64_t start_rank_, stop_rank_;
  virtual void DoInitial() override;
};

class ZRemrangebyscoreCmd : public Cmd {
 public:
  ZRemrangebyscoreCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRemrangebyscoreCmd(*this);
  }
 private:
  std::string key_;
  double min_score_, max_score_;
  bool left_close_, right_close_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ =  right_close_ = true;
  }
};

class ZRemrangebylexCmd : public Cmd {
 public:
  ZRemrangebylexCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), left_close_(true), right_close_(true) {}
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZRemrangebylexCmd(*this);
  }
 private:
  std::string key_;
  std::string min_member_, max_member_;
  bool left_close_, right_close_;
  virtual void DoInitial() override;
  virtual void Clear() {
    left_close_ =  right_close_ = true;
  }
};

class ZPopmaxCmd : public Cmd {
 public:
  ZPopmaxCmd(const std::string& name, int arity, uint16_t flag)
       : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const { 
    std::vector<std::string> res;
    res.emplace_back(key_);
    return res; 
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZPopmaxCmd(*this);
  }
 private:
  virtual void DoInitial() override;
  std::string key_;
  int64_t count_;
};

class ZPopminCmd : public Cmd {
 public:
  ZPopminCmd(const std::string& name, int arity, uint16_t flag)
       : Cmd(name, arity, flag) {}
  virtual std::vector<std::string> current_key() const { 
    std::vector<std::string> res;
    res.push_back(key_);
    return res; 
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new ZPopminCmd(*this);
  }
 private:
  virtual void DoInitial() override;
  std::string key_;
  int64_t count_;
};

#endif
