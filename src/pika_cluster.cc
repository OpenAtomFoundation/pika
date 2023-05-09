// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_cluster.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "include/pika_table.h"

extern PikaReplicaManager* g_pika_rm;
extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

const std::string PkClusterInfoCmd::kSlotSection = "slot";
const std::string PkClusterInfoCmd::kTableSection = "table";

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
  } else if (!strcasecmp(argv_[2].data(), kTableSection.data())) {
    info_section_ = kInfoTable;
    if (!ParseInfoTableSubCmd()) {
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
      } else if (info_range_ == kRange) {
        // doesn't process error, if error return nothing
        ClusterInfoSlotRange(table_name_, slots_, &info);
      }
      break;
    case kInfoTable:
      if (info_range_ == kAll) {
        ClusterInfoTableAll(&info);
      } else if (info_range_ == kSingle) {
        ClusterInfoTable(&info);
      }
    default:
      break;
  }
  res_.AppendStringLen(info.size());
  res_.AppendContent(info);
  return;
}

void PkClusterInfoCmd::ClusterInfoTableAll(std::string* info) {
  std::stringstream tmp_stream;
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  std::unordered_map<std::string, QpsStatistic> table_stat = g_pika_server->ServerAllTableStat();
  for (const auto& table_struct : table_structs) {
    std::string table_id = table_struct.table_name.substr(2);
    tmp_stream << "table_id: " << table_id << "\r\n";
    tmp_stream << "  partition_num: " << table_struct.partition_num << "\r\n";
    QpsStatistic qps = table_stat[table_struct.table_name];
    tmp_stream << "  total_commands_processed:" << qps.querynum.load() << "\r\n";
    tmp_stream << "  total_write_commands_processed:" << qps.write_querynum.load() << "\r\n";
    tmp_stream << "  qps: " << qps.last_sec_querynum << "\r\n";
    tmp_stream << "  write_qps: " << qps.last_sec_write_querynum << "\r\n";
    tmp_stream << "  read_qps: " << qps.last_sec_querynum - qps.last_sec_write_querynum << "\r\n";
  }
  info->append(tmp_stream.str());
}

void PkClusterInfoCmd::ClusterInfoTable(std::string* info) {
  std::stringstream tmp_stream;
  std::vector<TableStruct> table_structs = g_pika_conf->table_structs();
  for (const auto& table_struct : table_structs) {
    if (table_struct.table_name == table_name_) {
      std::string table_id = table_struct.table_name.substr(2);
      tmp_stream << "table_id: " << table_id << "\r\n";
      tmp_stream << "  partition_num: " << table_struct.partition_num << "\r\n";
      QpsStatistic qps = g_pika_server->ServerTableStat(table_name_);
      tmp_stream << "  qps: " << qps.last_sec_querynum << "\r\n";
      tmp_stream << "  write_qps: " << qps.last_sec_write_querynum << "\r\n";
      tmp_stream << "  read_qps: " << qps.last_sec_querynum - qps.last_sec_write_querynum << "\r\n";
      break;
    }
  }
  info->append(tmp_stream.str());
}

bool PkClusterInfoCmd::ParseInfoSlotSubCmd() {
  if (argv_.size() > 3) {
    if (argv_.size() == 4) {
      info_range_ = kRange;
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
      if (!ParseSlotGroup(slot_num_str, &slots_).ok()) {
        res_.SetRes(CmdRes::kInvalidParameter, kCmdNamePkClusterInfo);
        return false;
      }
    } else {
      res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterInfo);
      return false;
    }
  }
  return true;
}

bool PkClusterInfoCmd::ParseInfoTableSubCmd() {
  if (argv_.size() == 3) {
    info_range_ = kAll;
  } else if (argv_.size() == 4) {
    std::string tmp(argv_[3]);
    int64_t table_id;
    if (!pstd::string2int(tmp.c_str(), tmp.size(), &table_id)) {
      res_.SetRes(CmdRes::kInvalidParameter, kCmdNamePkClusterInfo);
      return false;
    }
    table_name_ = "db" + tmp;
    info_range_ = kSingle;
  } else if (argv_.size() > 4) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterInfo);
    return false;
  }
  return true;
}

void PkClusterInfoCmd::ClusterInfoSlotRange(const std::string& table_name, const std::set<uint32_t> slots,
                                            std::string* info) {
  std::stringstream tmp_stream;
  for (const auto& partition_id : slots) {
    std::string p_info;
    Status s = GetSlotInfo(table_name, partition_id, &p_info);
    if (!s.ok()) {
      continue;
    }
    tmp_stream << p_info;
  }
  info->append(tmp_stream.str());
}

void PkClusterInfoCmd::ClusterInfoSlotAll(std::string* info) {
  std::stringstream tmp_stream;
  for (const auto& table_item : g_pika_server->tables_) {
    pstd::RWLock partition_rwl(&table_item.second->partitions_rw_, false);
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

Status PkClusterInfoCmd::GetSlotInfo(const std::string table_name, uint32_t partition_id, std::string* info) {
  std::shared_ptr<SyncMasterPartition> partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!partition) {
    return Status::NotFound("not found");
  }
  Status s;
  std::stringstream tmp_stream;

  // binlog offset section
  uint32_t filenum = 0;
  uint64_t offset = 0;
  partition->Logger()->GetProducerStatus(&filenum, &offset);
  tmp_stream << partition->PartitionName() << " binlog_offset=" << filenum << " " << offset;

  // safety purge section
  std::string safety_purge;
  std::shared_ptr<SyncMasterPartition> master_partition =
      g_pika_rm->GetSyncMasterPartitionByName(PartitionInfo(table_name, partition_id));
  if (!master_partition) {
    LOG(WARNING) << "Sync Master Partition: " << table_name << ":" << partition_id << ", NotFound";
    s = Status::NotFound("SyncMasterPartition NotFound");
  } else {
    master_partition->GetSafetyPurgeBinlog(&safety_purge);
  }
  tmp_stream << ",safety_purge=" << (s.ok() ? safety_purge : "error") << "\r\n";

  if (g_pika_conf->consensus_level()) {
    LogOffset last_log = master_partition->ConsensusLastIndex();
    tmp_stream << "  consensus_last_log=" << last_log.ToString() << "\r\n";
  }

  // partition info section
  std::string p_info;
  s = g_pika_rm->GetPartitionInfo(table_name, partition_id, &p_info);
  if (!s.ok()) {
    return s;
  }
  tmp_stream << p_info;
  info->append(tmp_stream.str());
  return Status::OK();
}

Status ParseSlotGroup(const std::string& slot_group, std::set<uint32_t>* slots) {
  std::set<uint32_t> tmp_slots;
  int64_t slot_idx, start_idx, end_idx;
  std::string::size_type pos;
  std::vector<std::string> elems;
  pstd::StringSplit(slot_group, COMMA, elems);
  for (const auto& elem : elems) {
    if ((pos = elem.find("-")) == std::string::npos) {
      if (!pstd::string2int(elem.data(), elem.size(), &slot_idx) || slot_idx < 0) {
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
        if (!pstd::string2int(start_pos.data(), start_pos.size(), &start_idx) ||
            !pstd::string2int(end_pos.data(), end_pos.size(), &end_idx) || start_idx < 0 || end_idx < 0 ||
            start_idx > end_idx) {
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
    res_.SetRes(CmdRes::kErrOther, "PkClusterAddSlots/PkClusterDelSlots only support on sharding mode");
    return;
  }

  Status s = ParseSlotGroup(argv_[2], &slots_);
  if (!s.ok()) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
  if (argv_.size() == 3) {
    table_name_ = g_pika_conf->default_table();
  } else if (argv_.size() == 4) {
    int64_t table_id;
    if (!pstd::string2int(argv_[3].data(), argv_[3].size(), &table_id)) {
      res_.SetRes(CmdRes::kErrOther, "syntax error");
      return;
    }
    table_name_ = "db";
    table_name_ += std::to_string(table_id);
  } else {
    res_.SetRes(CmdRes::kSyntaxErr, "too many argument");
    return;
  }
  for (const auto& slot_id : slots_) {
    p_infos_.insert(PartitionInfo(table_name_, slot_id));
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
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name_);
  if (!table_ptr) {
    res_.SetRes(CmdRes::kErrOther, "Internal error: table not found!");
    return;
  }

  SlotState expected = INFREE;
  if (!std::atomic_compare_exchange_strong(&g_pika_server->slot_state_, &expected, INBUSY)) {
    res_.SetRes(CmdRes::kErrOther, "Slot in syncing or a change operation is under way, retry later");
    return;
  }

  bool pre_success = true;
  Status s = AddSlotsSanityCheck();
  if (!s.ok()) {
    LOG(WARNING) << "Addslots sanity check failed: " << s.ToString();
    pre_success = false;
  }
  if (pre_success) {
    s = g_pika_conf->AddTablePartitions(table_name_, slots_);
    if (!s.ok()) {
      LOG(WARNING) << "Addslots add to pika conf failed: " << s.ToString();
      pre_success = false;
    }
  }
  if (pre_success) {
    s = table_ptr->AddPartitions(slots_);
    if (!s.ok()) {
      LOG(WARNING) << "Addslots add to table partition failed: " << s.ToString();
      pre_success = false;
    }
  }
  if (pre_success) {
    s = g_pika_rm->AddSyncPartition(p_infos_);
    if (!s.ok()) {
      LOG(WARNING) << "Addslots add to sync partition failed: " << s.ToString();
      pre_success = false;
    }
  }

  g_pika_server->slot_state_.store(INFREE);

  if (!pre_success) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }

  res_.SetRes(CmdRes::kOk);
  LOG(INFO) << "Pika meta file overwrite success";
}

Status PkClusterAddSlotsCmd::AddSlotsSanityCheck() {
  Status s = g_pika_conf->TablePartitionsSanityCheck(table_name_, slots_, true);
  if (!s.ok()) {
    return s;
  }

  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name_);
  if (!table_ptr) {
    return Status::NotFound("table not found!");
  }

  for (uint32_t id : slots_) {
    std::shared_ptr<Partition> partition_ptr = table_ptr->GetPartitionById(id);
    if (partition_ptr != nullptr) {
      return Status::Corruption("partition " + std::to_string(id) + " already exist");
    }
  }
  s = g_pika_rm->AddSyncPartitionSanityCheck(p_infos_);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

/* pkcluster delslots 0-3,8-11
 * pkcluster delslots 0-3,8,9,10,11
 * pkcluster delslots 0,2,4,6,8,10,12,14
 */
void PkClusterDelSlotsCmd::DoInitial() {
  SlotParentCmd::DoInitial();
  if (!res_.ok()) {
    return;
  }
}

void PkClusterDelSlotsCmd::Do(std::shared_ptr<Partition> partition) {
  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name_);
  if (!table_ptr) {
    res_.SetRes(CmdRes::kErrOther, "Internal error: default table not found!");
    return;
  }

  SlotState expected = INFREE;
  if (!std::atomic_compare_exchange_strong(&g_pika_server->slot_state_, &expected, INBUSY)) {
    res_.SetRes(CmdRes::kErrOther, "Slot in syncing or a change operation is under way, retry later");
    return;
  }

  bool pre_success = true;
  Status s = RemoveSlotsSanityCheck();
  if (!s.ok()) {
    LOG(WARNING) << "Removeslots sanity check failed: " << s.ToString();
    pre_success = false;
  }
  // remove order maters
  if (pre_success) {
    s = g_pika_conf->RemoveTablePartitions(table_name_, slots_);
    if (!s.ok()) {
      LOG(WARNING) << "Removeslots remove from pika conf failed: " << s.ToString();
      pre_success = false;
    }
  }
  if (pre_success) {
    s = g_pika_rm->RemoveSyncPartition(p_infos_);
    if (!s.ok()) {
      LOG(WARNING) << "Remvoeslots remove from sync partition failed: " << s.ToString();
      pre_success = false;
    }
  }
  if (pre_success) {
    s = table_ptr->RemovePartitions(slots_);
    if (!s.ok()) {
      LOG(WARNING) << "Removeslots remove from table partition failed: " << s.ToString();
      pre_success = false;
    }
  }

  g_pika_server->slot_state_.store(INFREE);

  if (!pre_success) {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
    return;
  }
  res_.SetRes(CmdRes::kOk);
  LOG(INFO) << "Pika meta file overwrite success";
}

Status PkClusterDelSlotsCmd::RemoveSlotsSanityCheck() {
  Status s = g_pika_conf->TablePartitionsSanityCheck(table_name_, slots_, false);
  if (!s.ok()) {
    return s;
  }

  std::shared_ptr<Table> table_ptr = g_pika_server->GetTable(table_name_);
  if (!table_ptr) {
    return Status::NotFound("table not found");
  }

  for (uint32_t id : slots_) {
    std::shared_ptr<Partition> partition_ptr = table_ptr->GetPartitionById(id);
    if (partition_ptr == nullptr) {
      return Status::Corruption("partition " + std::to_string(id) + " not found");
    }
  }
  s = g_pika_rm->RemoveSyncPartitionSanityCheck(p_infos_);
  if (!s.ok()) {
    return s;
  }
  return Status::OK();
}

/* pkcluster slotsslaveof no one  [0-3,8-11 | all] [table_id]
 * pkcluster slotsslaveof ip port [0-3,8,9,10,11 | all] [table_id]
 * pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force
 * pkcluster slotsslaveof ip port [0,2,4,6,7,8,9 | all] force [table_id]
 */
void PkClusterSlotsSlaveofCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, kCmdNamePkClusterSlotsSlaveof);
    return;
  }
  if (g_pika_conf->classic_mode()) {
    res_.SetRes(CmdRes::kErrOther, "PkClusterSlotSync only support on sharding mode");
    return;
  }

  if (!strcasecmp(argv_[2].data(), "no") && !strcasecmp(argv_[3].data(), "one")) {
    is_none_ = true;
  } else {
    ip_ = argv_[2];
    if (!pstd::string2int(argv_[3].data(), argv_[3].size(), &port_) || port_ <= 0) {
      res_.SetRes(CmdRes::kInvalidInt);
      return;
    }

    if ((ip_ == "127.0.0.1" || ip_ == g_pika_server->host()) && port_ == g_pika_server->port()) {
      res_.SetRes(CmdRes::kErrOther, "You fucked up");
      return;
    }
  }

  bool all_slots = false;
  if (!strcasecmp(argv_[4].data(), "all")) {
    // not know which table yet
    all_slots = true;
  } else {
    Status s = ParseSlotGroup(argv_[4], &slots_);
    if (!s.ok()) {
      res_.SetRes(CmdRes::kErrOther, s.ToString());
      return;
    }
  }

  int64_t table_id;
  switch (argv_.size()) {
    case 5:
      table_name_ = g_pika_conf->default_table();
      break;
    case 6:
      if (!strcasecmp(argv_[5].data(), "force")) {
        force_sync_ = true;
        table_name_ = g_pika_conf->default_table();
      } else if (pstd::string2int(argv_[5].data(), argv_[5].size(), &table_id)) {
        table_name_ = "db";
        table_name_ += std::to_string(table_id);
      } else {
        res_.SetRes(CmdRes::kErrOther, "syntax error");
        return;
      }
      break;
    case 7:
      if ((!strcasecmp(argv_[5].data(), "force")) && (pstd::string2int(argv_[6].data(), argv_[6].size(), &table_id))) {
        force_sync_ = true;
        table_name_ = "db";
        table_name_ += std::to_string(table_id);
      } else {
        res_.SetRes(CmdRes::kErrOther, "syntax error");
        return;
      }
      break;
    default:
      res_.SetRes(CmdRes::kErrOther, "syntax error");
      return;
  }

  if (!g_pika_server->IsTableExist(table_name_)) {
    res_.SetRes(CmdRes::kInvalidTable);
    return;
  }

  if (all_slots) {
    slots_ = g_pika_server->GetTablePartitionIds(table_name_);
  }

  if (slots_.empty()) {
    res_.SetRes(CmdRes::kErrOther, "Slots set empty");
  }
}

void PkClusterSlotsSlaveofCmd::Do(std::shared_ptr<Partition> partition) {
  std::vector<uint32_t> to_del_slots;
  for (const auto& slot : slots_) {
    std::shared_ptr<SyncSlavePartition> slave_partition =
        g_pika_rm->GetSyncSlavePartitionByName(PartitionInfo(table_name_, slot));
    if (!slave_partition) {
      res_.SetRes(CmdRes::kErrOther, "Slot " + std::to_string(slot) + " not found!");
      return;
    }
    if (is_none_) {
      // check okay
    } else if (slave_partition->State() == ReplState::kConnected && slave_partition->MasterIp() == ip_ &&
               slave_partition->MasterPort() == port_) {
      to_del_slots.push_back(slot);
    }
  }

  for (const auto& to_del : to_del_slots) {
    slots_.erase(to_del);
  }

  Status s = Status::OK();
  ReplState state = force_sync_ ? ReplState::kTryDBSync : ReplState::kTryConnect;
  for (const auto& slot : slots_) {
    std::shared_ptr<SyncSlavePartition> slave_partition =
        g_pika_rm->GetSyncSlavePartitionByName(PartitionInfo(table_name_, slot));
    if (slave_partition->State() == ReplState::kConnected) {
      s = g_pika_rm->SendRemoveSlaveNodeRequest(table_name_, slot);
    }
    if (!s.ok()) {
      break;
    }
    if (slave_partition->State() != ReplState::kNoConnect) {
      // reset state
      slave_partition->SetReplState(ReplState::kNoConnect);
    }
    if (is_none_) {
    } else {
      s = g_pika_rm->ActivateSyncSlavePartition(RmNode(ip_, port_, table_name_, slot), state);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (s.ok()) {
    res_.SetRes(CmdRes::kOk);
  } else {
    res_.SetRes(CmdRes::kErrOther, s.ToString());
  }
}