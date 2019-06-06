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
    : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
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


#endif  // PIKA_SLOT_H_
