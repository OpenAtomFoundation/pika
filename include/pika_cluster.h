// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLUSTER_H_
#define PIKA_CLUSTER_H_

#include "include/pika_command.h"

class PkClusterInfoCmd : public Cmd {
 public:
  enum InfoSection {
    kInfoErr = 0x0,
    kInfoSlot
  };
  enum InfoRange {
    kSingle = 0x0,
    kAll
  };
  PkClusterInfoCmd(const std::string& name, int arity, uint16_t flag)
    : Cmd(name, arity, flag),
      info_section_(kInfoErr), info_range_(kAll), partition_id_(0) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);

 private:
  InfoSection info_section_;
  InfoRange info_range_;

  std::string table_name_;
  uint32_t partition_id_;

  virtual void DoInitial() override;
  virtual void Clear() {
    info_section_ = kInfoErr;
    info_range_ = kAll;
    table_name_.clear();
    partition_id_ = 0;
  }
  const static std::string kSlotSection;
  void ClusterInfoSlotAll(std::string* info);
  Status GetSlotInfo(const std::string table_name, uint32_t partition_id, std::string* info);
  bool ParseInfoSlotSubCmd();
};

class SlotParentCmd : public Cmd {
 public:
  SlotParentCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
 protected:
  std::set<uint32_t> slots_;
  std::set<PartitionInfo> p_infos_;
  virtual void DoInitial();
  virtual void Clear() {
    slots_.clear();
    p_infos_.clear();
  }
};

class PkClusterAddSlotsCmd : public SlotParentCmd {
 public:
  PkClusterAddSlotsCmd(const std::string& name, int arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class PkClusterRemoveSlotsCmd : public SlotParentCmd {
 public:
  PkClusterRemoveSlotsCmd(const std::string& name, int32_t arity, uint16_t flag)
      : SlotParentCmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
 private:
  virtual void DoInitial() override;
};

class PkClusterSlotSlaveofCmd : public Cmd {
 public:
  PkClusterSlotSlaveofCmd(const std::string& name , int arity, uint16_t flag)
      : Cmd(name, arity, flag) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
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
  }
};

#endif  // PIKA_CLUSTER_H_
