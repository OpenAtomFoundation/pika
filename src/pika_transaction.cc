// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <memory>

#include "include/pika_transaction.h"
#include "include/pika_admin.h"
#include "include/pika_client_conn.h"
#include "include/pika_define.h"
#include "include/pika_list.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "src/pstd/include/scope_record_lock.h"

extern std::unique_ptr<PikaServer> g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;

void MultiCmd::Do() {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (conn == nullptr || client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "MULTI calls can not be nested");
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

void ExecCmd::Do() {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  std::vector<CmdRes> res_vec = {};
  std::vector<std::shared_ptr<std::string>> resp_strs;
  for (size_t i = 0; i < cmds_.size(); ++i) {
    resp_strs.emplace_back(std::make_shared<std::string>());
  }
  auto resp_strs_iter = resp_strs.begin();
  std::for_each(cmds_.begin(), cmds_.end(), [&client_conn, &res_vec, &resp_strs_iter](CmdInfo& each_cmd_info) {
    each_cmd_info.cmd_->SetResp(*resp_strs_iter++);
    auto& cmd = each_cmd_info.cmd_;
    auto& db = each_cmd_info.db_;
    auto sync_db = each_cmd_info.sync_db_;
    cmd->res() = {};
    if (cmd->name() == kCmdNameFlushall) {
      auto flushall = std::dynamic_pointer_cast<FlushallCmd>(cmd);
      flushall->FlushAllWithoutLock();
      client_conn->SetTxnFailedIfKeyExists();
    } else if (cmd->name() == kCmdNameFlushdb) {
      auto flushdb = std::dynamic_pointer_cast<FlushdbCmd>(cmd);
      flushdb->DoWithoutLock();
      if (cmd->res().ok()) {
        cmd->res().SetRes(CmdRes::kOk);
      }
      client_conn->SetTxnFailedIfKeyExists(each_cmd_info.db_->GetDBName());
    } else {
      cmd->Do();
      if (cmd->res().ok() && cmd->is_write()) {
        cmd->DoBinlog();
        auto db_keys = cmd->current_key();
        for (auto& item : db_keys) {
          item = cmd->db_name().append(item);
        }
        if (cmd->IsNeedUpdateCache()) {
          cmd->DoUpdateCache();
        }
        client_conn->SetTxnFailedFromKeys(db_keys);
      }
    }
    res_vec.emplace_back(cmd->res());
  });

  res_.AppendArrayLen(res_vec.size());
  for (auto& r : res_vec) {
    res_.AppendStringRaw(r.message());
  }
}

void ExecCmd::Execute() {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (!client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::kErrOther, "EXEC without MULTI");
    return;
  }
  if (IsTxnFailedAndSetState()) {
    client_conn->ExitTxn();
    return;
  }
  SetCmdsVec();
  Lock();
  Do();

  Unlock();
  ServeToBLrPopWithKeys();
  list_cmd_.clear();
  client_conn->ExitTxn();
}

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

bool ExecCmd::IsTxnFailedAndSetState() {
  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn->IsTxnInitFailed()) {
    res_.SetRes(CmdRes::kTxnAbort, "Transaction discarded because of previous errors.");
    return true;
  }
  if (client_conn->IsTxnWatchFailed()) {
    res_.AppendStringLen(-1);
    return true;
  }
  return false;
}

void ExecCmd::Lock() {
  g_pika_server->DBLockShared();
  std::for_each(lock_db_.begin(), lock_db_.end(), [](auto& need_lock_db) {
    need_lock_db->DBLock();
  });
  if (is_lock_rm_dbs_) {
    g_pika_rm->DBLock();
  }

  std::for_each(r_lock_dbs_.begin(), r_lock_dbs_.end(), [this](auto& need_lock_db) {
    if (lock_db_keys_.count(need_lock_db) != 0) {
      pstd::lock::MultiRecordLock record_lock(need_lock_db->LockMgr());
      record_lock.Lock(lock_db_keys_[need_lock_db]);
    }
    need_lock_db->DBLockShared();
  });
}

void ExecCmd::Unlock() {
  std::for_each(r_lock_dbs_.begin(), r_lock_dbs_.end(), [this](auto& need_lock_db) {
    if (lock_db_keys_.count(need_lock_db) != 0) {
      pstd::lock::MultiRecordLock record_lock(need_lock_db->LockMgr());
      record_lock.Unlock(lock_db_keys_[need_lock_db]);
    }
    need_lock_db->DBUnlockShared();
  });
  if (is_lock_rm_dbs_) {
    g_pika_rm->DBUnlock();
  }
  std::for_each(lock_db_.begin(), lock_db_.end(), [](auto& need_lock_db) {
    need_lock_db->DBUnlock();
  });
  g_pika_server->DBUnlockShared();
}

void ExecCmd::SetCmdsVec() {
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(GetConn());
  auto cmd_que = client_conn->GetTxnCmdQue();

  while (!cmd_que.empty()) {
    auto cmd = cmd_que.front();
    auto cmd_db = client_conn->GetCurrentTable();
    auto db = g_pika_server->GetDB(cmd_db);
    auto sync_db = g_pika_rm->GetSyncMasterDBByName(DBInfo(cmd->db_name()));
    cmds_.emplace_back(cmd, db, sync_db);
    if (cmd->name() == kCmdNameSelect) {
      cmd->Do();
    } else if (cmd->name() == kCmdNameFlushdb) {
      is_lock_rm_dbs_ = true;
      lock_db_.emplace(g_pika_server->GetDB(cmd_db));
    } else if (cmd->name() == kCmdNameFlushall) {
      is_lock_rm_dbs_ = true;
      for (const auto& db_item : g_pika_server->GetDB()) {
        lock_db_.emplace(db_item.second);
      }
    } else {
      r_lock_dbs_.emplace(db);
      if (lock_db_keys_.count(db) == 0) {
        lock_db_keys_.emplace(db, std::vector<std::string>{});
      }
      auto cmd_keys = cmd->current_key();
      lock_db_keys_[db].insert(lock_db_keys_[db].end(), cmd_keys.begin(), cmd_keys.end());
      if (cmd->name() == kCmdNameLPush || cmd->name() == kCmdNameRPush) {
        list_cmd_.insert(list_cmd_.end(), cmds_.back());
      }
    }
    cmd_que.pop();
  }
}

void ExecCmd::ServeToBLrPopWithKeys() {
  for (auto each_list_cmd : list_cmd_) {
    auto push_keys = each_list_cmd.cmd_->current_key();
    //PS: currently, except for blpop/brpop, there are three cmds inherited from BlockingBaseCmd: lpush, rpush, rpoplpush
    //For rpoplpush which has 2 keys（source and receiver), push_keys[0] fetchs the receiver, push_keys[1] fetchs the source.(see RpopLpushCmd::current_key()
    auto push_key = push_keys[0];
    if (auto push_list_cmd = std::dynamic_pointer_cast<BlockingBaseCmd>(each_list_cmd.cmd_);
        push_list_cmd != nullptr) {
      push_list_cmd->TryToServeBLrPopWithThisKey(push_key, each_list_cmd.db_);
    }
  }
}

void WatchCmd::Execute() {
  Do();
}

void WatchCmd::Do() {
  auto mp = std::map<storage::DataType, storage::Status>{};
  for (const auto& key : keys_) {
    auto type_count = db_->storage()->IsExist(key, &mp);
    if (type_count > 1) {
      res_.SetRes(CmdRes::CmdRet::kErrOther, "EXEC WATCH watch key must be unique");
      return;
    }
    mp.clear();
  }


  auto conn = GetConn();
  auto client_conn = std::dynamic_pointer_cast<PikaClientConn>(conn);
  if (client_conn == nullptr) {
    res_.SetRes(CmdRes::kErrOther, name());
    return;
  }
  if (client_conn->IsInTxn()) {
    res_.SetRes(CmdRes::CmdRet::kErrOther, "WATCH inside MULTI is not allowed");
    return;
  }
  client_conn->AddKeysToWatch(db_keys_);
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
    db_keys_.push_back(db_name() + "_" + argv_[pos++]);
  }
}

void UnwatchCmd::Do() {
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

void DiscardCmd::Do() {
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
  client_conn->ExitTxn();
  res_.SetRes(CmdRes::CmdRet::kOk);
}
