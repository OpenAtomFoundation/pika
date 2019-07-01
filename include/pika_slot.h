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
 private:
  virtual void DoInitial() override;
};

class SlotsHashKeyCmd : public Cmd {
 public:
  SlotsHashKeyCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtSlotAsyncCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagSlotAsyncCmd : public Cmd {
 public:
  SlotsMgrtTagSlotAsyncCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag), dest_port_(0), slot_num_(-1) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
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

class SlotParentCmd : public Cmd {
 public:
  SlotParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
 protected:
  std::set<uint32_t> slots_;
  virtual void DoInitial();
  virtual void Clear() {
    slots_.clear();
  }
};

class AddSlotsCmd : public SlotParentCmd {
 public:
  AddSlotsCmd(const std::string& name, int arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class RemoveSlotsCmd : public SlotParentCmd {
 public:
  RemoveSlotsCmd(const std::string& name, int32_t arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotSyncCmd : public Cmd {
 public:
  SlotSyncCmd(const std::string& name , int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  std::string ip_;
  int64_t port_;
  int64_t slot_id_;
  bool force_sync_;
  bool is_noone_;
  virtual void DoInitial() override;
  virtual void Clear() {
    ip_.clear();
    port_ = 0;
    slot_id_ = 0;
    force_sync_ = false;
    is_noone_ = false;
  }
};

class SlotsScanCmd : public Cmd {
 public:
  SlotsScanCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag), pattern_("*"), count_(10) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
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
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtAsyncCancelCmd : public Cmd {
 public:
  SlotsMgrtAsyncCancelCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtSlotCmd : public Cmd {
 public:
  SlotsMgrtSlotCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagSlotCmd : public Cmd {
 public:
  SlotsMgrtTagSlotCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtOneCmd : public Cmd {
 public:
  SlotsMgrtOneCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class SlotsMgrtTagOneCmd : public Cmd {
 public:
  SlotsMgrtTagOneCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

#endif  // PIKA_SLOT_H_
