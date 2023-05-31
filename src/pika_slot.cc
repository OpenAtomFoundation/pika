// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_slot.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_table.h"
#include "include/pika_define.h"

extern PikaCmdDBManager* g_pika_cmd_db_manager;
extern PikaReplicaManager* g_pika_rm;
extern PikaServer* g_pika_server;

// SLOTSINFO
void SlotsInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsInfo);
    return;
  }

  return;
}

void SlotsInfoCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<DB> db_ptr = g_pika_server->GetDB(g_pika_conf->default_db());
  if (!db_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsInfo);
    return;
  }
  db_ptr->KeyScan();
  // this get will get last time scan info
  KeyScanInfo key_scan_info = db_ptr->GetKeyScanInfo();

  std::map<uint32_t, KeyScanInfo> infos;
  Status s = db_ptr->GetSlotsKeyScanInfo(&infos);
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
    return;
  }

  return;
}

void SlotsHashKeyCmd::Do(std::shared_ptr<Slot> slot) {
  res_.AppendArrayLen(argv_.size() - 1);
  std::shared_ptr<DB> db_ptr = g_pika_server->GetDB(g_pika_conf->default_db());
  if (!db_ptr) {
    res_.SetRes(CmdRes::kInvalidParameter, kCmdNameSlotsHashKey);
    return;
  }
  uint32_t slot_num = db_ptr->SlotNum();
  // iter starts from real key, first item in argv_ is command name
  std::vector<std::string>::const_iterator iter = argv_.begin() + 1;
  for (; iter != argv_.end(); iter++) {
    res_.AppendInteger(g_pika_cmd_db_manager->DistributeKey(*iter, slot_num));
  }
  return;
}

// slotsmgrtslot-async host port timeout maxbulks maxbytes slot numkeys
void SlotsMgrtSlotAsyncCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsMgrtSlotAsync);
    return;
  }

  return;
}

void SlotsMgrtSlotAsyncCmd::Do(std::shared_ptr<Slot> slot) {
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
    return;
  }

  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;  // Remember the first args is the opt name
  dest_ip_ = *it++;
  pstd::StringToLower(dest_ip_);

  std::string str_dest_port = *it++;
  if (!pstd::string2int(str_dest_port.data(), str_dest_port.size(), &dest_port_) || dest_port_ <= 0) {
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

  std::shared_ptr<DB> db = g_pika_server->GetDB(db_name_);
  if (db == nullptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsMgrtTagSlotAsync);
    return;
  }

  if (!pstd::string2int(str_slot_num.data(), str_slot_num.size(), &slot_num_) || slot_num_ < 0 ||
      slot_num_ >= HASH_SLOTS_SIZE) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsMgrtTagSlotAsync);
    return;
  }

  std::string str_keys_num = *it++;
  return;
}

void SlotsMgrtTagSlotAsyncCmd::Do(std::shared_ptr<Slot> slot) {
  int64_t moved = 0;
  int64_t remained = 0;
  // check if this slave node exist.
  // if exist, dont mark migrate done
  // cache coming request in codis proxy and keep retrying
  // Until sync done, new node slaveof no one.
  // mark this migrate done
  // proxy retry cached request in new node
  bool is_exist = true;
  std::shared_ptr<SyncMasterSlot> master_slot =
      g_pika_rm->GetSyncMasterSlotByName(SlotInfo(db_name_, slot_num_));
  if (!master_slot) {
    LOG(WARNING) << "Sync Master Slot: " << db_name_ << ":" << slot_num_ << ", NotFound";
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsMgrtTagSlotAsync);
    return;
  }
  is_exist = master_slot->CheckSlaveNodeExist(dest_ip_, dest_port_);
  if (is_exist) {
    remained = 1;
  } else {
    remained = 0;
  }
  res_.AppendArrayLen(2);
  res_.AppendInteger(moved);
  res_.AppendInteger(remained);
}

// SLOTSSCAN slotnum cursor [COUNT count]
void SlotsScanCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNameSlotsScan);
    return;
  }
  int64_t slotnum;
  if (!pstd::string2int(argv_[1].data(), argv_[1].size(), &slotnum)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsScan);
    return;
  }
  slotnum_ = static_cast<uint32_t>(slotnum);
  if (!pstd::string2int(argv_[2].data(), argv_[2].size(), &cursor_)) {
    res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsScan);
    return;
  }
  size_t argc = argv_.size(), index = 3;

  while (index < argc) {
    std::string opt = argv_[index];
    if (!strcasecmp(opt.data(), "match") || !strcasecmp(opt.data(), "count")) {
      index++;
      if (index >= argc) {
        res_.SetRes(CmdRes::kSyntaxErr);
        return;
      }
      if (!strcasecmp(opt.data(), "match")) {
        pattern_ = argv_[index];
      } else if (!pstd::string2int(argv_[index].data(), argv_[index].size(), &count_) || count_ <= 0) {
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

void SlotsScanCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<DB> db_ptr = g_pika_server->GetDB(g_pika_conf->default_db());
  if (!db_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsScan);
    return;
  }
  std::shared_ptr<Slot> cur_slot = db_ptr->GetSlotById(slotnum_);
  if (!cur_slot) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsScan);
    return;
  }
  std::vector<std::string> keys;
  int64_t cursor_ret = cur_slot->db()->Scan(storage::DataType::kAll, cursor_, pattern_, count_, &keys);

  res_.AppendArrayLen(2);

  char buf[32];
  int len = pstd::ll2string(buf, sizeof(buf), cursor_ret);
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
    return;
  }
  // iter starts from real key, first item in argv_ is command name
  std::vector<std::string>::const_iterator iter = argv_.begin() + 1;
  for (; iter != argv_.end(); iter++) {
    int64_t slotnum;
    if (!pstd::string2int(iter->data(), iter->size(), &slotnum)) {
      res_.SetRes(CmdRes::kInvalidInt, kCmdNameSlotsDel);
      return;
    }
    slots_.push_back(static_cast<uint32_t>(slotnum));
  }
  return;
}

void SlotsDelCmd::Do(std::shared_ptr<Slot> slot) {
  std::shared_ptr<DB> db_ptr = g_pika_server->GetDB(g_pika_conf->default_db());
  if (!db_ptr) {
    res_.SetRes(CmdRes::kNotFound, kCmdNameSlotsDel);
    return;
  }
  if (db_ptr->IsKeyScaning()) {
    res_.SetRes(CmdRes::kErrOther, "The keyscan operation is executing, Try again later");
    return;
  }
  std::vector<uint32_t> successed_slots;
  for (auto& slotnum : slots_) {
    std::shared_ptr<Slot> cur_slot = db_ptr->GetSlotById(slotnum);
    if (!cur_slot) {
      continue;
    }
    cur_slot->FlushDB();
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
    return;
  }
  PikaCmdArgsType::const_iterator it = argv_.begin() + 1;
  key_ = *it++;
  // pstd::StringToLower(key_);
  return;
}

void SlotsMgrtExecWrapperCmd::Do(std::shared_ptr<Slot> slot) {
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
    return;
  }

  return;
}

void SlotsMgrtAsyncStatusCmd::Do(std::shared_ptr<Slot> slot) {
  std::string status;
  std::string ip = "none";
  int64_t port = -1, slots = -1, moved = -1, remained = -1;
  std::string mstatus = "no";
  res_.AppendArrayLen(5);
  status = "dest server: " + ip + ":" + std::to_string(port);
  res_.AppendStringLen(status.size());
  res_.AppendContent(status);
  status = "slot number: " + std::to_string(slots);
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
    return;
  }

  return;
}

void SlotsMgrtAsyncCancelCmd::Do(std::shared_ptr<Slot> slot) {
  res_.SetRes(CmdRes::kOk);
  return;
}

// slotsmgrtslot host port timeout slot
void SlotsMgrtSlotCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtSlot + " NOT supported");
  return;
}

void SlotsMgrtSlotCmd::Do(std::shared_ptr<Slot> slot) { return; }

// slotsmgrttagslot host port timeout slot
void SlotsMgrtTagSlotCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtTagSlot + " NOT supported");
  return;
}

void SlotsMgrtTagSlotCmd::Do(std::shared_ptr<Slot> slot) { return; }

// slotsmgrtone host port timeout key
void SlotsMgrtOneCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtOne + " NOT supported");
  return;
}

void SlotsMgrtOneCmd::Do(std::shared_ptr<Slot> slot) { return; }

// slotsmgrttagone host port timeout key
void SlotsMgrtTagOneCmd::DoInitial() {
  res_.SetRes(CmdRes::kErrOther, kCmdNameSlotsMgrtTagOne + " NOT supported");
  return;
}

void SlotsMgrtTagOneCmd::Do(std::shared_ptr<Slot> slot) { return; }
