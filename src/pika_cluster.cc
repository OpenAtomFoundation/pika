// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_rm.h"
#include "include/pika_table.h"
#include "include/pika_server.h"
#include "include/pika_cluster.h"
#include "include/pika_cmd_table_manager.h"

extern PikaReplicaManager* g_pika_rm;
extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

const std::string PkClusterInfoCmd::kSlotSection = "slot";

// pkcluster info slot table:slot
// pkcluster info table
// pkcluster info node
// pkcluster info cluster
void PkClusterInfoCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterInfo);
    return;
  }
  if (g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "PkClusterInfo only support on sharding mode");
    return;
  }
  if (!strcasecmp(argv_[2].data(), kSlotSection.data())) {
    info_section_ = kInfoSlot;
    if (!ParseInfoSlotSubCmd()) {
      return;
    }
  } else {
    info_section_ = kInfoErr;
  }
  return;
}

void PkClusterInfoCmd::Do(std::shared_ptr<Partition> partition) {
  std::string info;
  switch (info_section_) {
    case kInfoSlot:
      if (info_range_ == kAll) {
        ClusterInfoSlotAll(&info);
      } else if (info_range_ == kSingle) {
        // doesn't process error, if error return nothing
        GetSlotInfo(table_name_, partition_id_, &info);
      }
      break;
    default:
      break;
  }
  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}

bool PkClusterInfoCmd::ParseInfoSlotSubCmd() {
  if (argv_.size() > 3) {
    if (argv_.size() == 4) {
      info_range_ = kSingle;
      std::string tmp(argv_[3]);
      size_t pos = tmp.find(':');
      std::string slot_num_str;
      if (pos == std::string::npos) {
        table_name_ = g_pika_conf->default_table();
        slot_num_str = tmp;
      } else {
        table_name_ = tmp.substr(0, pos);
        slot_num_str = tmp.substr(pos + 1);
      }
      unsigned long partition_id;
      if (!slash::string2ul(slot_num_str.c_str(), slot_num_str.size(), &partition_id)) {
        res_.SetRes(CmdRes::kInvalidParameter, kCmdNamePkClusterInfo);
        return false;
      }
      partition_id_ = partition_id;
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterInfo);
      return false;
    }
  }
  return true;
}

void PkClusterInfoCmd::ClusterInfoSlotAll(std::string* info) {
  std::stringstream tmp_stream;
  for (const auto& table_item : g_pika_server->tables_) {
    slash::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
    for (const auto& partition_item : table_item.second->partitions_) {
      std::string table_name = table_item.second->GetTableName();
      uint32_t partition_id = partition_item.second->GetPartitionId();
      std::string p_info;
      Status s = GetSlotInfo(table_name, partition_id, &p_info);
      if (!s.ok()) {
        continue;
      }
      tmp_stream << p_info;
    }
  }
  info->append(tmp_stream.str());
}

Status PkClusterInfoCmd::GetSlotInfo(const std::string table_name,
    uint32_t partition_id, std::string* info) {
  std::shared_ptr<Partition> partition =
    g_pika_server->GetTablePartitionById(table_name, partition_id);
  if (!partition) {
    return Status::NotFound("not found");
  }
  uint32_t filenum = 0;
  uint64_t offset = 0;
  std::stringstream tmp_stream;
  partition->logger()->GetProducerStatus(&filenum, &offset);
  tmp_stream << partition->GetPartitionName() << " binlog_offset="
    << filenum << " " << offset << "\r\n";
  std::string p_info;
  Status s = g_pika_rm->GetPartitionInfo(table_name, partition_id, &p_info);
  if (!s.ok()) {
    return s;
  }
  tmp_stream << p_info;
  info->append(tmp_stream.str());
  return Status::OK();
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
    res_.SetRes(CmdRes::kErrOther, "PkClusterAddSlots/PkClusterRemoveSlots only support on sharding mode");
    return;
  }

  Status s = ParseSlotGroup(argv_[2], &slots_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  std::string table_name = g_pika_conf->default_table();
  for (const auto& slot_id : slots_) {
    p_infos_.insert(PartitionInfo(table_name, slot_id));
  }
}

/*
 * pkcluster addslots 0-3,8-11
 * pkcluster addslots 0-3,8,9,10,11
 * pkcluster addslots 0,2,4,6,8,10,12,14
 */
void PkClusterAddSlotsCmd::DoInitial() {
  SlotParentCmd::DoInitial();
  if (!res_.ok()) {
    return;
  }
}

void PkClusterAddSlotsCmd::Do(std::shared_ptr<Partition> partition) {
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

/* pkcluster removeslots 0-3,8-11
 * pkcluster removeslots 0-3,8,9,10,11
 * pkcluster removeslots 0,2,4,6,8,10,12,14
 */
void PkClusterRemoveSlotsCmd::DoInitial() {
  SlotParentCmd::DoInitial();
  if (!res_.ok()) {
    return;
  }
}

void PkClusterRemoveSlotsCmd::Do(std::shared_ptr<Partition> partition) {
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

/* pkcluster slotslaveof no one  0-3,8-11
 * pkcluster slotslaveof ip port 0-3,8,9,10,11
 * pkcluster slotslaveof ip port 0,2,4,6 force
 */
void PkClusterSlotSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterSlotSlaveof);
    return;
  }
  if (g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "PkClusterSlotSync only support on sharding mode");
    return;
  }

  if (!strcasecmp(argv_[2].data(), "no")
    && !strcasecmp(argv_[3].data(), "one")) {
    is_noone_ = true;
  } else {
    ip_ = argv_[2];
    if (!slash::string2l(argv_[3].data(), argv_[3].size(), &port_)
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

  Status s = ParseSlotGroup(argv_[4], &slots_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }

  if (argv_.size() == 5) {
    // do nothing
  } else if (argv_.size() == 6
    && !strcasecmp(argv_[5].data(), "force")) {
    force_sync_ = true;
  } else {
    res_.SetRes(CmdRes::kSyntaxErr);
  }
}

void PkClusterSlotSlaveofCmd::Do(std::shared_ptr<Partition> partition) {
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

