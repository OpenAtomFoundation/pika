// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_LIST_H_
#define PIKA_LIST_H_

#include "storage/storage.h"

#include "include/pika_command.h"
#include "include/pika_partition.h"
#include "net/src/dispatch_thread.h"

/*
 * list
 */
class LIndexCmd : public Cmd {
 public:
  LIndexCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), index_(0){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LIndexCmd(*this); }

 private:
  std::string key_;
  int64_t index_ = 0;
  virtual void DoInitial() override;
  virtual void Clear() { index_ = 0; }
};

class LInsertCmd : public Cmd {
 public:
  LInsertCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), dir_(storage::After){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LInsertCmd(*this); }

 private:
  std::string key_;
  storage::BeforeOrAfter dir_;
  std::string pivot_;
  std::string value_;
  virtual void DoInitial() override;
};

class LLenCmd : public Cmd {
 public:
  LLenCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LLenCmd(*this); }

 private:
  std::string key_;
  virtual void DoInitial() override;
};

class BLRPopBaseCmd : public Cmd {
 public:
  BLRPopBaseCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void BlockThisClientToWaitLRPush(net::BlockPopType block_pop_type);

 protected:
  std::vector<std::string> keys_;
  int64_t expire_time_{0};
};

class BLPopCmd : public BLRPopBaseCmd {
 public:
  BLPopCmd(const std::string& name, int arity, uint16_t flag) : BLRPopBaseCmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res = keys_;
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new BLPopCmd(*this); }

 private:
  virtual void DoInitial() override;
};

class LPopCmd : public Cmd {
 public:
  LPopCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LPopCmd(*this); }

 private:
  std::string key_;
  virtual void DoInitial() override;
};

class BPopServeCmd : public Cmd {
 public:
  BPopServeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag) {}
  void TryToServeBLrPopWithThisKey(const std::string& key, std::shared_ptr<Partition> partition);
  static void ServeAndUnblockConns(void* args);
};

class LPushCmd : public BPopServeCmd {
 public:
  LPushCmd(const std::string& name, int arity, uint16_t flag) : BPopServeCmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LPushCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial() override;
  virtual void Clear() { values_.clear(); }
};

class LPushxCmd : public Cmd {
 public:
  LPushxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LPushxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  virtual void DoInitial() override;
};

class LRangeCmd : public Cmd {
 public:
  LRangeCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), left_(0), right_(0){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LRangeCmd(*this); }

 private:
  std::string key_;
  int64_t left_ = 0;
  int64_t right_ = 0;
  virtual void DoInitial() override;
};

class LRemCmd : public Cmd {
 public:
  LRemCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), count_(0){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LRemCmd(*this); }

 private:
  std::string key_;
  int64_t count_ = 0;
  std::string value_;
  virtual void DoInitial() override;
};

class LSetCmd : public Cmd {
 public:
  LSetCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), index_(0){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LSetCmd(*this); }

 private:
  std::string key_;
  int64_t index_ = 0;
  std::string value_;
  virtual void DoInitial() override;
};

class LTrimCmd : public Cmd {
 public:
  LTrimCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag), start_(0), stop_(0){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new LTrimCmd(*this); }

 private:
  std::string key_;
  int64_t start_ = 0;
  int64_t stop_ = 0;
  virtual void DoInitial() override;
};

class BRPopCmd : public BLRPopBaseCmd {
 public:
  BRPopCmd(const std::string& name, int arity, uint16_t flag) : BLRPopBaseCmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res = keys_;
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new BRPopCmd(*this); }

 private:
  virtual void DoInitial() override;
};

class RPopCmd : public Cmd {
 public:
  RPopCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new RPopCmd(*this); }

 private:
  std::string key_;
  virtual void DoInitial() override;
};

class RPopLPushCmd : public BPopServeCmd {
 public:
  RPopLPushCmd(const std::string& name, int arity, uint16_t flag) : BPopServeCmd(name, arity, flag){};
  std::vector<std::string> current_key() const override {
    std::vector<std::string> res;
    res.push_back(source_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new RPopLPushCmd(*this); }

 private:
  std::string source_;
  std::string receiver_;
  virtual void DoInitial() override;
};

class RPushCmd : public BPopServeCmd {
 public:
  RPushCmd(const std::string& name, int arity, uint16_t flag) : BPopServeCmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new RPushCmd(*this); }

 private:
  std::string key_;
  std::vector<std::string> values_;
  virtual void DoInitial() override;
  virtual void Clear() { values_.clear(); }
};

class RPushxCmd : public Cmd {
 public:
  RPushxCmd(const std::string& name, int arity, uint16_t flag) : Cmd(name, arity, flag){};
  virtual std::vector<std::string> current_key() const {
    std::vector<std::string> res;
    res.push_back(key_);
    return res;
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new RPushxCmd(*this); }

 private:
  std::string key_;
  std::string value_;
  virtual void DoInitial() override;
};
#endif
