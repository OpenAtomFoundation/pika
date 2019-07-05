// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rm.h"
#include "include/pika_slot.h"
#include "include/pika_table.h"
#include "include/pika_server.h"
#include "include/pika_cmd_table_manager.h"

extern PikaCmdTableManager* g_pika_cmd_table_manager;
extern PikaReplicaManager* g_pika_rm;
extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

// SLOTSINFO
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

// SLOTSHASHKEY key1 [key2 …]
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

// SLOTSMGRTTAGSLOT host port timeout maxbulks maxbytes slot numkeys
void SlotsMgrtSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtSlotAsync);
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

// SLOTSMGRTTAGSLOT-ASYNC host port timeout maxbulks maxbytes slot numkeys
void SlotsMgrtTagSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtTagSlotAsync);
  }

  PikaCmdArgsType::const_iterator it = argv_.begin() + 1; //Remember the first args is the opt name
  dest_ip_ = *it++;
  slash::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!slash::string2l(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsMgrtTagSlotAsync);
    return;
  }

  if ((dest_ip_ == "127.0.0.1" || dest_ip_ == g_pika_server->host()) && dest_port_ == g_pika_server->port()) {
    res_.SetRes(CmdRes::kErrOther, "destination address error");
    return;
  }

  std::string str_timeout_ms = *it++;

  std::string str_max_bulks = *it++;

  std::string str_max_bytes_ = *it++;

  std::string str_slot_num = *it++;
  if (!slash::string2l(str_slot_num.data(), str_slot_num.size(), &slot_num_)
      || slot_num_ < 0 || slot_num_ >= g_pika_conf->default_partition_num()) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsMgrtTagSlotAsync);
    return;
  }

  std::string str_keys_num = *it++;
  return;
}

void SlotsMgrtTagSlotAsyncCmd::Do(std::shared_ptr<Partition> partition) {
  int64_t moved = 0;
  int64_t remained = 0;
  // check if this slave node exist.
  // if exist, dont mark migrate done
  // cache coming request in codis proxy and keep retrying
  // Until sync done, new node slaveof no one.
  // mark this migrate done
  // proxy retry cached request in new node
  bool is_exist = g_pika_rm->CheckPartitionSlaveExist(
      RmNode(dest_ip_, dest_port_, g_pika_conf->default_table(), slot_num_));
  if (is_exist) {
    remained = 1;
  } else {
    remained = 0;
  }
  res_.AppendArrayLen(2);
  res_.AppendInteger(moved);
  res_.AppendInteger(remained);
}

Status ParseSlotGroup(const std::string& slot_group,
                      std::set<uint32_t>* slots) {
  std::set<uint32_t> tmp_slots;
  int64_t slot_idx, start_idx, end_idx;
  std::string::size_type pos;
  std::vector<std::string> elems;
  slash::StringSplit(slot_group, COMMA, elems);
  for (const auto& elem :  elems) {
    if ((pos = elem.find("-")) == std::string::npos) {
      if (!slash::string2l(elem.data(), elem.size(), &slot_idx)
        || slot_idx < 0) {
        return Status::Corruption("syntax error");
      } else {
        tmp_slots.insert(static_cast<uint32_t>(slot_idx));
      }
    } else {
      if (pos == 0 || pos == (elem.size() - 1)) {
        return Status::Corruption("syntax error");
      } else {
        std::string start_pos = elem.substr(0, pos);
        std::string end_pos = elem.substr(pos + 1, elem.size() - pos);
        if (!slash::string2l(start_pos.data(), start_pos.size(), &start_idx)
          || !slash::string2l(end_pos.data(), end_pos.size(), &end_idx)
          || start_idx < 0 || end_idx < 0 || start_idx > end_idx) {
          return Status::Corruption("syntax error");
        }
        for (int64_t idx = start_idx; idx <= end_idx; ++idx) {
          tmp_slots.insert(static_cast<uint32_t>(idx));
        }
      }
    }
  }
  slots->swap(tmp_slots);
  return Status::OK();
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

  Status s = ParseSlotGroup(argv_[1], &slots_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  std::string table_name = g_pika_conf->default_table();
  for (const auto& slot_id : slots_) {
    p_infos_.insert(PartitionInfo(table_name, slot_id));
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
    g_pika_rm->AddSyncPartition(p_infos_);
    LOG(INFO) << "Pika meta file overwrite success";
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  g_pika_server->slot_state_.store(INFREE);
}

/* removeslots 0-3,8-11
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
    g_pika_rm->RemoveSyncPartition(p_infos_);
    LOG(INFO) << "Pika meta file overwrite success";
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  g_pika_server->slot_state_.store(INFREE);
}

/* slotslaveof no one  0-3,8-11
 * slotslaveof ip port 0-3,8,9,10,11
 * slotslaveof ip port 0,2,4,6 force
 */
void SlotSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotSlaveof);
    return;
  }
  if (g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "SlotSync only support on sharding mode");
    return;
  }

  if (!strcasecmp(argv_[1].data(), "no")
    && !strcasecmp(argv_[2].data(), "one")) {
    is_noone_ = true;
  } else {
    ip_ = argv_[1];
    if (!slash::string2l(argv_[2].data(), argv_[2].size(), &port_)
      || port_ <= 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }

    if ((ip_ == "127.0.0.1" || ip_ == g_pika_server->host())
      && port_ == g_pika_server->port()) {
      res_.SetRes(CmdRes::kErrOther, "you fucked up");
      return;
    }
  }

  Status s = ParseSlotGroup(argv_[3], &slots_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  if (argv_.size() == 4) {
    // do nothing
  } else if (argv_.size() == 5
    && !strcasecmp(argv_[4].data(), "force")) {
    force_sync_ = true;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void SlotSlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  std::string table_name = g_pika_conf->default_table();
  for (const auto& slot : slots_) {
    std::shared_ptr<SyncSlavePartition> slave_partition =
        g_pika_rm->GetSyncSlavePartitionByName(
                PartitionInfo(table_name, slot));
    if (!slave_partition) {
      res_.SetRes(CmdRes::kErrOther, "slot " + std::to_string(slot) + " not found!");
      return;
    }
    if (is_noone_) {
      // check okay
    } else if (slave_partition->State() != ReplState::kNoConnect
      && slave_partition->State() != ReplState::kError) {
      res_.SetRes(CmdRes::kErrOther, "slot " + std::to_string(slot) + " in syncing");
      return;
    }
  }

  Status s;
  ReplState state = force_sync_
    ? ReplState::kTryDBSync : ReplState::kTryConnect;
  for (const auto& slot : slots_) {
    if (is_noone_) {
      std::shared_ptr<SyncSlavePartition> slave_partition =
        g_pika_rm->GetSyncSlavePartitionByName(
                PartitionInfo(table_name, slot));
      if (slave_partition->State() == ReplState::kConnected) {
        s = g_pika_rm->SendRemoveSlaveNodeRequest(table_name, slot);
      } else {
        // reset state
        s = g_pika_rm->SetSlaveReplState(
            PartitionInfo(table_name, slot), ReplState::kNoConnect);
      }
    } else {
      s = g_pika_rm->ActivateSyncSlavePartition(
          RmNode(ip_, port_, table_name, slot), state);
    }
    if (!s.ok()) {
      break;
    }
  }

  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}

// SLOTSSCAN slotnum cursor [COUNT count]
void SlotsScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  int64_t slotnum;
  if (!slash::string2l(argv_[1].data(), argv_[1].size(), &slotnum)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsScan);
    return;
  }
  slotnum_ = static_cast<uint32_t>(slotnum);
  if (!slash::string2l(argv_[2].data(), argv_[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsScan);
    return;
  }
  size_t argc = argv_.size(), index = 3;

  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match")
      || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!slash::string2l(argv_[index].data(), argv_[index].size(), &count_) || count_ <= 0) {
        res_.SetRes(CmdRes::kInvalidInt);
        return;
      }
    } else {
      res_.SetRes(CmdRes::kSyntaxErr);
      return;
    }
    index++;
  }
  return;
}

void SlotsScanCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(g_pika_conf->default_table());
  if (!table_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsScan);
    return;
  }
  std::shared_ptr<Partition> cur_partition = table_ptr->GetPartitionById(slotnum_);
  if (!cur_partition) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsScan);
    return;
  }
  std::vector<std::string> keys;
  int64_t cursor_ret = cur_partition->db()->Scan(cursor_, pattern_, count_, &keys);

  res_.AppendArrayLen(2);

  char buf[32];
  int len = slash::ll2string(buf, sizeof(buf), cursor_ret);
  res_.AppendStringLen(len);
  res_.AppendContent(buf);

  res_.AppendArrayLen(keys.size());
  std::vector<std::string>::iterator iter;
  for (iter = keys.begin(); iter != keys.end(); iter++) {
    res_.AppendStringLen(iter->size());
    res_.AppendContent(*iter);
  }
  return;
}

// SLOTSDEL slot1 [slot2 …]
void SlotsDelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsDel);
  }
  // iter starts from real key, first item in argv_ is command name
  std::vector<std::string>::const_iterator iter = argv_.begin() + 1;
  for (; iter != argv_.end(); iter++) {
    int64_t slotnum;
    if (!slash::string2l(iter->data(), iter->size(), &slotnum)) {
      res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsDel);
      return;
    }
    slots_.push_back(static_cast<uint32_t>(slotnum));
  }
  return;
}

void SlotsDelCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(g_pika_conf->default_table());
  if (!table_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsDel);
    return;
  }
  if (table_ptr->IsKeyScaning()) {
    res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
    return;
  }
  std::vector<uint32_t> successed_slots;
  for (auto& slotnum : slots_) {
    std::shared_ptr<Partition> cur_partition = table_ptr->GetPartitionById(slotnum);
    if (!cur_partition) {
      continue;
    }
    cur_partition->FlushDB();
    successed_slots.push_back(slotnum);
  }
  res_.AppendArrayLen(successed_slots.size());
  for (auto& slotnum : successed_slots) {
    res_.AppendArrayLen(2);
    res_.AppendInteger(slotnum);
    res_.AppendInteger(0);
  }
  return;
}

// SLOTSMGRT-EXEC-WRAPPER $hashkey $command [$arg1 ...]
void SlotsMgrtExecWrapperCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
   res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtExecWrapper);
  }
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
  key_ = *it++;
  //slash::StringToLower(key_);
  return;
}

void SlotsMgrtExecWrapperCmd::Do(std::shared_ptr<Partition> partition) {
  // return 0 means proxy will request to new slot server
  // return 1 means proxy will keey trying
  // return 2 means return this key directly
  res_.AppendArrayLen(2);
  res_.AppendInteger(1);
  res_.AppendInteger(1);
  return;
}

// slotsmgrt-async-status
void SlotsMgrtAsyncStatusCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncStatus);
  }
  return;
}

void SlotsMgrtAsyncStatusCmd::Do(std::shared_ptr<Partition> partition) {
  std::string status;
  std::string ip = "none";
  int64_t port = -1, slot = -1, moved = -1, remained = -1;
  std::string mstatus = "no";
  res_.AppendArrayLen(5);
  status = "dest server: " + ip + ":" + std::to_string(port);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "slot number: " + std::to_string(slot);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "migrating  : " + mstatus;
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "moved keys : " + std::to_string(moved);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "remain keys: " + std::to_string(remained);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  return;
}

// slotsmgrt-async-cancel
void SlotsMgrtAsyncCancelCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtAsyncCancel);
  }
  return;
}

void SlotsMgrtAsyncCancelCmd::Do(std::shared_ptr<Partition> partition) {
  res_.SetRes(CmdRes::kOk);
  return;
}

// slotsmgrtslot host port timeout slot
void SlotsMgrtSlotCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtSlot + " NOT supported");
  return;
}

void SlotsMgrtSlotCmd::Do(std::shared_ptr<Partition> partition) {
  return;
}

// slotsmgrttagslot host port timeout slot
void SlotsMgrtTagSlotCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtTagSlot + " NOT supported");
  return;
}

void SlotsMgrtTagSlotCmd::Do(std::shared_ptr<Partition> partition) {
  return;
}

// slotsmgrtone host port timeout key
void SlotsMgrtOneCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtOne + " NOT supported");
  return;
}

void SlotsMgrtOneCmd::Do(std::shared_ptr<Partition> partition) {
  return;
}

// slotsmgrttagone host port timeout key
void SlotsMgrtTagOneCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtTagOne + " NOT supported");
  return;
}

void SlotsMgrtTagOneCmd::Do(std::shared_ptr<Partition> partition) {
  return;
}
