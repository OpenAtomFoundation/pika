// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CLUSTER_H_
#define PIKA_CLUSTER_H_

#include "include/pika_command.h"

Status ParseSlotGroup(const std::string& slot_group, std::set<uint32_t>* slots);

class PkClusterInfoCmd : public Cmd {
 public:
  enum InfoSection { kInfoErr = 0x0, kInfoSlot, kInfoTable };
  enum InfoRange { kSingle = 0x0, kAll, kRange };
  PkClusterInfoCmd(const std::string& name, int arity, uint16_t flag)
      : Cmd(name, arity, flag), info_section_(kInfoErr), info_range_(kAll) {}
  virtual void Do(std::shared_ptr<Partition> partition = nullptr);
  virtual void Split(std::shared_ptr<Partition> partition, const HintKeys& hint_keys){};
  virtual void Merge(){};
  virtual Cmd* Clone() override { return new PkClusterInfoCmd(*this); }

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
  void ClusterInfoSlotRange(const std::string& table_name, const std::set<uint32_t> slots, std::string* info);
  void ClusterInfoSlotAll(std::string* info);
  Status GetSlotInfo(const std::string table_name, uint32_t partition_id, std::string* info);
  bool ParseInfoSlotSubCmd();
  bool ParseInfoTableSubCmd();
};
#endif  // PIKA_CLUSTER_H_
