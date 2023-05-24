// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_transaction.h"
#include <memory>
#include "include/pika_client_conn.h"
#include "include/pika_define.h"

void MultiCmd::Do(std::shared_ptr<Partition> partition) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (conn == nullptr || client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "ERR MULTI calls can not be nested");
  }
  client_conn->SetTxnState(PikaClientConn::TxnState::Start);
  res_.SetRes(CmdRes::kOk);
}

void MultiCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
  res_.SetRes(CmdRes::kOk);
}


void ExecCmd::Do(std::shared_ptr<Partition> partition) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (!client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "EXECABORT Transaction discarded because of previous errors.");
    return;
  }

  if (client_conn->IsTxnInitFailed()) {
    res_.SetRes(CmdRes::kErrOther, "EXECABORT Transaction discarded because of previous errors.");
    client_conn->RemoveWatchedKeys();
    client_conn->SetTxnState(PikaClientConn::TxnState::None);
    return;
  }
  if (client_conn->IsTxnWatchFailed()) {
    res_.AppendStringLen(-1);
    client_conn->RemoveWatchedKeys();
    client_conn->SetTxnState(PikaClientConn::TxnState::None);
    return;
  }
  //! TODO(lee) : 这里应该加锁，原子地执行下面的所有指令
  // 这里不用管，因为已经加锁了，但是是记录锁，只是给某几个记录加锁了。
  // InternalProcessCommand函数中
  // 所以这里还得斟酌一下，看看是加表锁还是加记录锁
  auto cmd_res = client_conn->ExecTxnCmds();

  if (cmd_res.empty()) {
    res_.AppendStringLen(-1);
    return;
  }
  auto ret_string = std::string{};
  res_.AppendArrayLen(cmd_res.size());
  for (auto & cmd_re : cmd_res) {
    res_.AppendStringRaw(cmd_re.message());
//    const auto &ret = cmd_res[i];
//    ret_string.append(std::to_string(i) + ") "+ ret.message());
  }

//  res_.SetRes(CmdRes::CmdRet::kOk, ret_string);
}

void ExecCmd::DoInitial() {
  if (!CheckArg(argv_.size())) {
    res_.SetRes(CmdRes::kWrongNum, name());
    return;
  }
}



void WatchCmd::Do(std::shared_ptr<Partition> partition) {
  auto mp = std::map<storage::DataType, storage::Status>{};
  partition->db()->Exists(keys_, &mp);
  if (mp.size() > 1) {
    // 说明一个key里面有多种类型
    res_.SetRes(CmdRes::CmdRet::kErrOther, "watch key must be unique");
    return;
  }

  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
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
    table_keys_.push_back(table_name() + argv_[pos++]);
  }
}

void UnwatchCmd::Do(std::shared_ptr<Partition> partition) {
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

void DiscardCmd::Do(std::shared_ptr<Partition> partition) {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (!client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "DISCARD without MULTI");
    return;
  }
  client_conn->RemoveWatchedKeys();
  client_conn->SetTxnState(PikaClientConn::TxnState::None);
  res_.SetRes(CmdRes::CmdRet::kOk);
}
