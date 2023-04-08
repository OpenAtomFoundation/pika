// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLUSTER_H_
#define PIKA_CLUSTER_H_

#include "include/pika_command.h"


Status ParseSlotGroup(const std::string& slot_group,
    std::set<uint32_t>* slots);

class PkClusterInfoCmd : public Cmd {
 public:
  enum InfoSection {
    kInfoErr = 0x0,
    kInfoSlot,
    kInfoTable
  };
  enum InfoRange {
    kSingle = 0x0,
    kAll,
    kRange
  };
  PkClusterInfoCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag),
      info_section_(kInfoErr), info_range_(kAll) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  virtual Cmd* Clone() override {
    return new PkClusterInfoCmd(*this);
  }

 private:
  InfoSection info_section_;
  InfoRange info_range_;

  std::string table_name_;
  std::set<uint32_t> slots_;

  virtual void DoInitial() override;
  virtual void Clear() {
    info_section_ = kInfoErr;
    info_range_ = kAll;
    table_name_.clear();
    slots_.clear();
  }
  const static std::string kSlotSection;
  const static std::string kTableSection;
  void ClusterInfoTableAll(std::string* info);
  void ClusterInfoTable(std::string* info);
  void ClusterInfoSlotRange(const std::string& table_name, const std::set<uint32_t> slots,
      std::string* info);
  void ClusterInfoSlotAll(std::string* info);
  Status GetSlotInfo(const std::string table_name, uint32_t partition_id, std::string* info);
  bool ParseInfoSlotSubCmd();
  bool ParseInfoTableSubCmd();
};

class SlotParentCmd : public Cmd {
 public:
  SlotParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag)  {}

 protected:
  std::set<uint32_t> slots_;
  std::set<PartitionInfo> p_infos_;
  virtual void DoInitial();
  virtual void Clear() {
    slots_.clear();
    p_infos_.clear();
    table_name_.clear();
  }
};

class PkClusterAddSlotsCmd : public SlotParentCmd {
 public:
  PkClusterAddSlotsCmd(const std::string& name, int arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  virtual Cmd* Clone() override {
    return new PkClusterAddSlotsCmd(*this);
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
  Status AddSlotsSanityCheck();
};

class PkClusterDelSlotsCmd : public SlotParentCmd {
 public:
  PkClusterDelSlotsCmd(const std::string& name, int32_t arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  virtual Cmd* Clone() override {
    return new PkClusterDelSlotsCmd(*this);
  }
 private:
  virtual void DoInitial() override;
  Status RemoveSlotsSanityCheck();
};

class PkClusterSlotsSlaveofCmd : public Cmd {
 public:
  PkClusterSlotsSlaveofCmd(const std::string& name , int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  virtual Cmd* Clone() override {
    return new PkClusterSlotsSlaveofCmd(*this);
  }
 private:
  std::string ip_;
  int64_t port_;
  std::set<uint32_t> slots_;
  bool force_sync_;
  bool is_noone_;
  virtual void DoInitial() override;
  virtual void Clear() {
    ip_.clear();
    port_ = 0;
    slots_.clear();
    force_sync_ = false;
    is_noone_ = false;
    table_name_.clear();
  }
};


class PkClusterAddTableCmd : public Cmd {
 public:
  PkClusterAddTableCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), slot_num_(0) {}
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  Cmd* Clone() override {
    return new PkClusterAddTableCmd(*this);
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  int64_t  slot_num_;
  void DoInitial() override;
  Status AddTableSanityCheck();
  void Clear() override {
    slot_num_ = 0;
    table_name_.clear();
  }
};

class PkClusterDelTableCmd : public PkClusterDelSlotsCmd {
 public:
  PkClusterDelTableCmd(const std::string& name, int arity, uint16_t flag)
      : PkClusterDelSlotsCmd(name, arity, flag) {}
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys) {};
  virtual void Merge() {};
  Cmd* Clone() override {
    return new PkClusterDelTableCmd(*this);
  }
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  void DoInitial() override;
  Status DelTableSanityCheck(const std::string& table_name);
};

#endif  // PIKA_CLUSTER_H_
