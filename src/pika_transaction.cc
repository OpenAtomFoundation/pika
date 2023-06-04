// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_transaction.h"
#include <memory>
#include "include/pika_client_conn.h"
#include "include/pika_define.h"

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

  if (client_conn->IsTxnInitFailed()) {
    res_.SetRes(CmdRes::kErrOther, "EXEC ABORT Transaction discarded because of previous errors.");
    client_conn->ExitTxn();
    return;
  }
  if (client_conn->IsTxnWatchFailed()) {
    res_.AppendStringLen(-1);
    client_conn->ExitTxn();
    return;
  }
  auto cmd_res = client_conn->ExecTxnCmds();
  if (cmd_res.empty()) {
    res_.AppendStringLen(-1);
    client_conn->ExitTxn();
    return;
  }
  auto ret_string = std::string{};
  res_.AppendArrayLen(cmd_res.size());
  for (auto & cmd_re : cmd_res) {
    res_.AppendStringRaw(cmd_re.message());
  }
  client_conn->ExitTxn();
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

std::vector<std::string> ExecCmd::GetInvolvedSlots() {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return {};
  }
  return client_conn->GetTxnInvolvedDbs();
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
  client_conn->AddKeysToWatch(table_keys_);
  res_.SetRes(CmdRes::kOk);
}



void WatchCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  size_t pos = 1;
  while (pos < argv_.size()) {
    keys_.emplace_back(argv_[pos]);
    table_keys_.push_back(db_name() + argv_[pos++]);
  }
}

//NOTE(leeHao): 在redis中，如果unwatch出现在队列之中，其实不会生效
void UnwatchCmd::Do(std::shared_ptr<Slot> slot) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    if (client_conn->IsTxnFailed()) {
      return;
    }
    res_.AppendInteger(-1);
    return;
  }
  client_conn->RemoveWatchedKeys();
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
