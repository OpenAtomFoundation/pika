// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slot.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_table.h"
#include "include/pika_server.h"

extern PikaCmdTableManager* g_pika_cmd_table_manager;
extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

void SlotsInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
  }
  return;
}

void SlotsInfoCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(g_pika_conf->default_table());
  if (!table_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsInfo);
    return;
  }
  table_ptr->KeyScan();
  // this get will get last time scan info
  KeyScanInfo key_scan_info = table_ptr->GetKeyScanInfo();

  std::map<uint32_t, KeyScanInfo> infos;
  Status s = table_ptr->GetPartitionsKeyScanInfo(&infos);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kInvalidParameter, kCmdNameSlotsInfo);
    return;
  }
  res_.AppendArrayLen(infos.size());
  for (auto& key_info : infos) {
    uint64_t total_key_size = 0;
    for (size_t idx = 0; idx < key_info.second.key_infos.size(); ++idx) {
      total_key_size += key_info.second.key_infos[idx].keys;
    }
    res_.AppendArrayLen(2);
    res_.AppendInteger(key_info.first);
    res_.AppendInteger(total_key_size);
  }
  return;
}

void SlotsHashKeyCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
  }
  return;
}

void SlotsHashKeyCmd::Do(std::shared_ptr<Partition> partition) {
  res_.AppendArrayLen(argv_.size() - 1);
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(g_pika_conf->default_table());
  uint32_t partition_num = table_ptr->PartitionNum();
  if (!table_ptr) {
    res_.SetRes(CmdRes::kInvalidParameter, kCmdNameSlotsHashKey);
  }
  // iter starts from real key, first item in argv_ is command name
  std::vector<std::string>::const_iterator iter = argv_.begin() + 1;
  for (; iter != argv_.end(); iter++) {
    res_.AppendInteger(g_pika_cmd_table_manager->DistributeKey(*iter, partition_num));
  }
  return;
}

void SlotsMgrtSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
  }
  return;
}

void SlotsMgrtSlotAsyncCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t moved = 0;
  int64_t remained = 0;
  res_.AppendArrayLen(2);
  res_.AppendInteger(moved);
  res_.AppendInteger(remained);
}

void SlotsMgrtTagSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsHashKey);
  }
  return;
}

void SlotsMgrtTagSlotAsyncCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t moved = 0;
  int64_t remained = 0;
  res_.AppendArrayLen(2);
  res_.AppendInteger(moved);
  res_.AppendInteger(remained);
}

void SlotParentCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kSyntaxErr);
    return;
  }
  if (g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "AddSlots/RemoveSlots only support on sharding mode");
    return;
  }

  int64_t slot_idx, start_idx, end_idx;
  std::string::size_type pos;
  std::vector<std::string> elems;
  slash::StringSplit(argv_[1], COMMA, elems);
  for (const auto& elem :  elems) {
    if ((pos = elem.find("-")) == std::string::npos) {
      if (!slash::string2l(elem.data(), elem.size(), &slot_idx)
        || slot_idx < 0) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        slots_.insert(static_cast<uint32_t>(slot_idx));
      }
    } else {
      if (pos == 0 || pos == (elem.size() - 1)) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      } else {
        std::string start_pos = elem.substr(0, pos);
        std::string end_pos = elem.substr(pos + 1, elem.size() - pos);
        if (!slash::string2l(start_pos.data(), start_pos.size(), &start_idx)
          || !slash::string2l(end_pos.data(), end_pos.size(), &end_idx)
          || start_idx < 0 || end_idx < 0 || start_idx > end_idx) {
          res_.SetRes(CmdRes::kSyntaxErr);
          return;
        }
        for (int64_t idx = start_idx; idx <= end_idx; ++idx) {
          slots_.insert(static_cast<uint32_t>(idx));
        }
      }
    }
  }
}

/*
 * addslots 0-3,8-11
 * addslots 0-3,8,9,10,11
 * addslots 0,2,4,6,8,10,12,14
 */
void AddSlotsCmd::DoInitial() {
  SlotParentCmd::DoInitial();
  if (!res_.ok()) {
    return;
  }
}

void AddSlotsCmd::Do(std::shared_ptr<Partition> partition) {
  std::string table_name = g_pika_conf->default_table();
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name);
  if (!table_ptr) {
    res_.SetRes(CmdRes::kErrOther, "Internal error: table not found!");
    return;
  }

  SlotState expected = INFREE;
  if (!std::atomic_compare_exchange_strong(&g_pika_server->slot_state_,
              &expected, INADDSLOTS)) {
    res_.SetRes(CmdRes::kErrOther,
            "Slot in syncing or a change operation is under way, retry later");
    return;
  }

  Status s = g_pika_conf->AddTablePartitions(table_name, slots_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    table_ptr->AddPartitions(slots_);
    LOG(INFO) << "Pika meta file overwrite success";
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  g_pika_server->slot_state_.store(INFREE);
}

/*
 * removeslots 0-3,8-11
 * removeslots 0-3,8,9,10,11
 * removeslots 0,2,4,6,8,10,12,14
 */
void RemoveSlotsCmd::DoInitial() {
  SlotParentCmd::DoInitial();
  if (!res_.ok()) {
    return;
  }
}

void RemoveSlotsCmd::Do(std::shared_ptr<Partition> partition) {
  std::string table_name = g_pika_conf->default_table();
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name);
  if (!table_ptr) {
    res_.SetRes(CmdRes::kErrOther, "Internal error: default table not found!");
    return;
  }

  SlotState expected = INFREE;
  if (!std::atomic_compare_exchange_strong(&g_pika_server->slot_state_,
              &expected, INREMOVESLOTS)) {
    res_.SetRes(CmdRes::kErrOther,
            "Slot in syncing or a change operation is under way, retry later");
    return;
  }

  Status s = g_pika_conf->RemoveTablePartitions(table_name, slots_);
  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
    table_ptr->RemovePartitions(slots_);
    LOG(INFO) << "Pika meta file overwrite success";
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  g_pika_server->slot_state_.store(INFREE);
}

