// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_SLOT_H_
#define PIKA_SLOT_H_

#include "include/pika_command.h"

class SlotsInfoCmd : public Cmd {
 public:
  SlotsInfoCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsInfoCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsHashKeyCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtSlotAsyncCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtSlotAsyncCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag), dest_port_(0), slot_num_(-1) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtTagSlotAsyncCmd(*this);
  }
 private:
  virtual void DoInitial() override;
  std::string dest_ip_;
  int64_t dest_port_;
  int64_t slot_num_;
  virtual void Clear() {
    dest_ip_.clear();
    dest_port_ = 0;
    slot_num_ = -1;
  }
};

class SlotsScanCmd : public Cmd {
 public:
  SlotsScanCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag), pattern_("*"), count_(10) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsScanCmd(*this);
  }
 private:
  int64_t cursor_;
  uint32_t slotnum_;
  std::string pattern_;
  int64_t count_;
  virtual void DoInitial() override;
  virtual void Clear() {
    pattern_ = "*";
    count_ = 10;
  }
};

class SlotsDelCmd : public Cmd {
 public:
  SlotsDelCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsDelCmd(*this);
  }
 private:
  std::vector<uint32_t> slots_;
  virtual void DoInitial() override;
  virtual void Clear() {
    slots_.clear();
  }
};

class SlotsMgrtExecWrapperCmd : public Cmd {
 public:
  SlotsMgrtExecWrapperCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtExecWrapperCmd(*this);
  }
 private:
  std::string key_;
  virtual void DoInitial() override;
  virtual void Clear() {
    key_.clear();
  }
};

class SlotsMgrtAsyncStatusCmd : public Cmd {
 public:
  SlotsMgrtAsyncStatusCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtAsyncStatusCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtAsyncCancelCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtSlotCmd : public Cmd {
 public:
  SlotsMgrtSlotCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtSlotCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtTagSlotCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtOneCmd : public Cmd {
 public:
  SlotsMgrtOneCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtOneCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual Cmd* Clone() override {
    return new SlotsMgrtTagOneCmd(*this);
  }
 private:
  virtual void DoInitial() override;
};

#endif  // PIKA_SLOT_H_
