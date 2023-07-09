// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_transaction.h"
#include <memory>
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "pstd_defer.h"

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

void MultiCmd::Do(std::shared_ptr<Slot> partition) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (conn == nullptr || client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "ERR MULTI calls can not be nested");
    return;
  }
  client_conn->SetTxnStartState(true);
  res_.SetRes(CmdRes::kOk);
}

void MultiCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
}


void ExecCmd::Do(std::shared_ptr<Slot> slot) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (!client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "ERR EXEC without MULTI");
    return;
  }
  DEFER {
    client_conn->ExitTxn();
  };

  if (client_conn->IsTxnInitFailed()) {
    res_.SetRes(CmdRes::kTxnAbort, "Transaction discarded because of previous errors.");
    return;
  }
  if (client_conn->IsTxnWatchFailed()) {
    res_.AppendStringLen(-1);
    return;
  }
  // TODO(leehao) 加锁，不太确定下面这个加锁方案行不行
  auto cmd_que = client_conn->GetTxnCmdQue();
  auto res_vec = std::vector<CmdRes>{};
  auto cmd_slot_vec = std::vector<std::pair<std::shared_ptr<Cmd>, std::shared_ptr<Slot>>>{};
  auto slot_set = std::unordered_set<std::shared_ptr<Slot>>{};  // 去重,以免重复给一个slot加锁
  // 先统一加锁
  while (!cmd_que.empty()) {
    auto cmd = cmd_que.front();
    auto cmd_db = client_conn->GetCurrentDb();  // 由于有可能是select命令，所以需要每次都从这个conn中去拿去最新的dbname
    auto cmd_slot = g_pika_server->GetSlotByDBName(cmd_db);
    if (slot_set.count(cmd_slot) == 0) {
      slot_set.emplace(cmd_slot);
    }
    cmd_slot_vec.emplace_back(cmd, cmd_slot);
    cmd_que.pop();
  }
  std::shared_lock l(g_pika_server->dbs_rw_);
  std::for_each(slot_set.begin(), slot_set.end(), [](auto &slot){
    slot->DbRWLockWriter();
  });

  // 再集中执行，最后再统一解锁
  std::for_each(cmd_slot_vec.begin(), cmd_slot_vec.end(), [&client_conn, &res_vec](auto& each_cmd) {
    auto& [cmd, cmd_slot] = each_cmd;
    std::shared_ptr<SyncMasterSlot> sync_slot =
        g_pika_rm->GetSyncMasterSlotByName(SlotInfo(cmd_slot->GetDBName(), cmd_slot->GetSlotID()));
    cmd->res() = {};
    cmd->Do(cmd_slot);
    if (cmd->res().ok() && cmd->is_write()) {
      cmd->DoBinlog(sync_slot);
      auto db_keys = cmd->current_key();
      for (auto& item : db_keys) {
        item = cmd->db_name().append(item);
      }
      client_conn->SetTxnFailedFromKeys(db_keys);
    }
    res_vec.emplace_back(cmd->res());
  });

  std::for_each(cmd_slot_vec.begin(), cmd_slot_vec.end(),[](auto &each_cmd) {
    auto cmd_slot = each_cmd.second;
    cmd_slot->DbRWUnLock();
  });
  res_.AppendArrayLen(res_vec.size());
  for (auto &r : res_vec) {
    res_.AppendStringRaw(r.message());
  }
}
// 如果是multi和exec的话，不应该写binlog
void ExecCmd::Execute() {
  std::shared_ptr<Slot> slot;
  slot = g_pika_server->GetSlotByDBName(db_name_);
  Do(slot);
}

//! 在这里还没法得到涉及到的key，因为客户端连接对象中有一些的key他们的db不一样
void ExecCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
}

void WatchCmd::Do(std::shared_ptr<Slot> slot) {
  auto mp = std::map<storage::DataType, storage::Status>{};
  slot->db()->Exists(keys_, &mp);
  if (mp.size() > 1) {
    // 说明一个key里面有多种类型
    res_.SetRes(CmdRes::CmdRet::kErrOther, "EXEC WATCH watch key must be unique");
    return;
  }

  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::CmdRet::kErrOther, "ERR WATCH inside MULTI is not allowed");
    return;
  }
  client_conn->AddKeysToWatch(db_keys_);
  res_.SetRes(CmdRes::kOk);
}

// 如果是multi和exec的话，不应该写binlog
void WatchCmd::Execute() {
    std::shared_ptr<Slot> slot;
    slot = g_pika_server->GetSlotByDBName(db_name_);
    Do(slot);
}

void WatchCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  size_t pos = 1;
  while (pos < argv_.size()) {
    keys_.emplace_back(argv_[pos]);
    db_keys_.push_back(db_name() + argv_[pos++]);
  }
}

void UnwatchCmd::Do(std::shared_ptr<Slot> slot) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsTxnExecing()) {
    res_.SetRes(CmdRes::CmdRet::kOk);
    return ;
  }
  client_conn->RemoveWatchedKeys();
  // 这里是因为其他客户端连接的时候会修改这个watch了的客户端连接，状态设置为WatchFailed
  // 那么这里得将WatchFailed状态设置为未失败
  if (client_conn->IsTxnWatchFailed()) {
    client_conn->SetTxnWatchFailState(false);
  }
  res_.SetRes(CmdRes::CmdRet::kOk);
}

void UnwatchCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
}

void DiscardCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
}

void DiscardCmd::Do(std::shared_ptr<Slot> partition) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (!client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "ERR DISCARD without MULTI");
    return;
  }
  client_conn->ExitTxn();
  res_.SetRes(CmdRes::CmdRet::kOk);
}
