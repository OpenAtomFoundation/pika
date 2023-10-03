/*
 * Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include "log.h"

#include <algorithm>

#include "client.h"
#include "cmd_context.h"
#include "command.h"
#include "config.h"
#include "pikiwidb.h"
#include "slow_log.h"
#include "store.h"

namespace pikiwidb {
PClient* PClient::s_current = 0;

std::set<std::weak_ptr<PClient>, std::owner_less<std::weak_ptr<PClient> > > PClient::s_monitors_;

int PClient::processInlineCmd(const char* buf, size_t bytes, std::vector<PString>& params) {
  if (bytes < 2) {
    return 0;
  }

  PString res;

  for (size_t i = 0; i + 1 < bytes; ++i) {
    if (buf[i] == '\r' && buf[i + 1] == '\n') {
      if (!res.empty()) {
        params.emplace_back(std::move(res));
      }

      return static_cast<int>(i + 2);
    }

    if (isblank(buf[i])) {
      if (!res.empty()) {
        params.reserve(4);
        params.emplace_back(std::move(res));
      }
    } else {
      res.reserve(16);
      res.push_back(buf[i]);
    }
  }

  return 0;
}

static int ProcessMaster(const char* start, const char* end) {
  auto state = PREPL.GetMasterState();

  switch (state) {
    case PReplState_connected:
      // discard all requests before sync;
      // or continue serve with old data? TODO
      return static_cast<int>(end - start);

    case PReplState_wait_auth:
      if (end - start >= 5) {
        if (strncasecmp(start, "+OK\r\n", 5) == 0) {
          PClient::Current()->SetAuth();
          return 5;
        } else {
          assert(!!!"check masterauth config, master password maybe wrong");
        }
      } else {
        return 0;
      }
      break;

    case PReplState_wait_replconf:
      if (end - start >= 5) {
        if (strncasecmp(start, "+OK\r\n", 5) == 0) {
          return 5;
        } else {
          assert(!!!"check error: send replconf command");
        }
      } else {
        return 0;
      }
      break;

    case PReplState_wait_rdb: {
      const char* ptr = start;
      // recv RDB file
      if (PREPL.GetRdbSize() == std::size_t(-1)) {
        ++ptr;  // skip $
        int s;
        if (PParseResult::ok == GetIntUntilCRLF(ptr, end - ptr, s)) {
          assert(s > 0);  // check error for your masterauth or master config

          PREPL.SetRdbSize(s);
          INFO("recv rdb size {}", s);
        }
      } else {
        std::size_t rdb = static_cast<std::size_t>(end - ptr);
        PREPL.SaveTmpRdb(ptr, rdb);
        ptr += rdb;
      }

      return static_cast<int>(ptr - start);
    } break;

    case PReplState_online:
      break;

    default:
      assert(!!!"wrong master state");
      break;
  }

  return -1;  // do nothing
}

int PClient::handlePacket(pikiwidb::TcpConnection* obj, const char* start, int bytes) {
  s_current = this;

  const char* const end = start + bytes;
  const char* ptr = start;

  if (isPeerMaster()) {
    //  check slave state
    auto recved = ProcessMaster(start, end);
    if (recved != -1) {
      return recved;
    }
  }

  auto parseRet = parser_.ParseRequest(ptr, end);
  if (parseRet == PParseResult::error) {
    if (!parser_.IsInitialState()) {
      tcp_connection_->ActiveClose();
      return 0;
    }

    // try inline command
    std::vector<PString> params;
    auto len = processInlineCmd(ptr, bytes, params);
    if (len == 0) {
      return 0;
    }

    ptr += len;
    parser_.SetParams(params);
    parseRet = PParseResult::ok;
  } else if (parseRet != PParseResult::ok) {
    return static_cast<int>(ptr - start);
  }

  DEFER { reset(); };

  // handle packet
  const auto& params = parser_.GetParams();
  if (params.empty()) {
    return static_cast<int>(ptr - start);
  }

  PString cmd(params[0]);
  std::transform(params[0].begin(), params[0].end(), cmd.begin(), ::tolower);

  if (!auth_) {
    if (cmd == "auth") {
      auto now = ::time(nullptr);
      if (now <= last_auth_ + 1) {
        // avoid guess password.
        tcp_connection_->ActiveClose();
        return 0;
      } else {
        last_auth_ = now;
      }
    } else {
      ReplyError(PError_needAuth, &reply_);
      return static_cast<int>(ptr - start);
    }
  }

  DEBUG("client {}, cmd {}", tcp_connection_->GetUniqueId(), cmd);

  PSTORE.SelectDB(db_);
  FeedMonitors(params);

  const PCommandInfo* info = PCommandTable::GetCommandInfo(cmd);

  if (!info) {  // 如果这个命令不存在，那么就走新的命令处理流程
    handlePacketNew(obj, params, cmd);
    return static_cast<int>(ptr - start);
  }

  // check transaction
  if (IsFlagOn(ClientFlag_multi)) {
    if (cmd != "multi" && cmd != "exec" && cmd != "watch" && cmd != "unwatch" && cmd != "discard") {
      if (!info->CheckParamsCount(static_cast<int>(params.size()))) {
        ERROR("queue failed: cmd {} has params {}", cmd, params.size());
        ReplyError(info ? PError_param : PError_unknowCmd, &reply_);
        FlagExecWrong();
      } else {
        if (!IsFlagOn(ClientFlag_wrongExec)) {
          queue_cmds_.push_back(params);
        }

        reply_.PushData("+QUEUED\r\n", 9);
        INFO("queue cmd {}", cmd);
      }

      return static_cast<int>(ptr - start);
    }
  }

  // check readonly slave and execute command
  PError err = PError_ok;
  if (PREPL.GetMasterState() != PReplState_none && !IsFlagOn(ClientFlag_master) &&
      (info->attr & PCommandAttr::PAttr_write)) {
    err = PError_readonlySlave;
    ReplyError(err, &reply_);
  } else {
    PSlowLog::Instance().Begin();
    err = PCommandTable::ExecuteCmd(params, info, IsFlagOn(ClientFlag_master) ? nullptr : &reply_);
    PSlowLog::Instance().EndAndStat(params);
  }

  if (err == PError_ok && (info->attr & PAttr_write)) {
    Propagate(params);
  }

  return static_cast<int>(ptr - start);
}

// 为了兼容老的命令处理流程，新的命令处理流程在这里
// 后面可以把client这个类重构，完整的支持新的命令处理流程
int PClient::handlePacketNew(pikiwidb::TcpConnection* obj, const std::vector<std::string>& params, const std::string& cmd) {
  auto cmdPtr = g_pikiwidb->CmdTableManager()->GetCommand(cmd);

  if (!cmdPtr) {
    ReplyError(PError_unknowCmd, &reply_);
    return 0;
  }

  if (!cmdPtr->CheckArg(params.size())) {
    ReplyError(PError_param, &reply_);
    return 0;
  }

  CmdContext ctx;
  ctx.client_ = this;
  // 因为 params 是一个引用，不能直接传给 ctx.argv_，所以需要拷贝一份，后面可以优化
  std::vector<std::string> argv = params;
  ctx.argv_ = argv;

  cmdPtr->Execute(ctx);

  reply_.PushData(ctx.message().data(), ctx.message().size());
  return 0;
}

PClient* PClient::Current() { return s_current; }

PClient::PClient(TcpConnection* obj) : tcp_connection_(obj), db_(0), flag_(0), name_("clientxxx") {
  auth_ = false;
  SelectDB(0);
  reset();
}

int PClient::HandlePackets(pikiwidb::TcpConnection* obj, const char* start, int size) {
  int total = 0;

  while (total < size) {
    auto processed = handlePacket(obj, start + total, size - total);
    if (processed <= 0) {
      break;
    }

    total += processed;
  }

  obj->SendPacketSafely(reply_.ReadAddr(), reply_.ReadableSize());
  reply_.Clear();
  return total;
}

void PClient::OnConnect() {
  if (isPeerMaster()) {
    PREPL.SetMasterState(PReplState_connected);
    PREPL.SetMaster(std::static_pointer_cast<PClient>(shared_from_this()));

    SetName("MasterConnection");
    SetFlag(ClientFlag_master);

    if (g_config.masterauth.empty()) {
      SetAuth();
    }
  } else {
    if (g_config.password.empty()) {
      SetAuth();
    }
  }
}

void PClient::Close() {
  if (tcp_connection_) {
    tcp_connection_->ActiveClose();
  }
}

bool PClient::SelectDB(int db) {
  if (PSTORE.SelectDB(db) >= 0) {
    db_ = db;
    return true;
  }

  return false;
}

void PClient::reset() {
  s_current = nullptr;
  parser_.Reset();
}

bool PClient::isPeerMaster() const {
  const auto& repl_addr = PREPL.GetMasterAddr();
  return repl_addr.GetIP() == tcp_connection_->GetPeerIp() && repl_addr.GetPort() == tcp_connection_->GetPeerPort();
}

bool PClient::Watch(int dbno, const PString& key) {
  DEBUG("Client {} watch {}, db {}", name_, key, dbno);
  return watch_keys_[dbno].insert(key).second;
}

bool PClient::NotifyDirty(int dbno, const PString& key) {
  if (IsFlagOn(ClientFlag_dirty)) {
    INFO("client is already dirty {}", tcp_connection_->GetUniqueId());
    return true;
  }

  if (watch_keys_[dbno].count(key)) {
    INFO("{} client become dirty because key {} in db {}", tcp_connection_->GetUniqueId(), key, dbno);
    SetFlag(ClientFlag_dirty);
    return true;
  } else {
    INFO("Dirty key is not exist: {}, because client unwatch before dirty", key);
  }

  return false;
}

bool PClient::Exec() {
  DEFER {
    this->ClearMulti();
    this->ClearWatch();
  };

  if (IsFlagOn(ClientFlag_wrongExec)) {
    return false;
  }

  if (IsFlagOn(ClientFlag_dirty)) {
    FormatNullArray(&reply_);
    return true;
  }

  PreFormatMultiBulk(queue_cmds_.size(), &reply_);
  for (const auto& cmd : queue_cmds_) {
    DEBUG("EXEC {}, for client {}", cmd[0], tcp_connection_->GetUniqueId());
    const PCommandInfo* info = PCommandTable::GetCommandInfo(cmd[0]);
    PError err = PCommandTable::ExecuteCmd(cmd, info, &reply_);

    // may dirty clients;
    if (err == PError_ok && (info->attr & PAttr_write)) {
      Propagate(cmd);
    }
  }

  return true;
}

void PClient::ClearMulti() {
  queue_cmds_.clear();
  ClearFlag(ClientFlag_multi);
  ClearFlag(ClientFlag_wrongExec);
}

void PClient::ClearWatch() {
  watch_keys_.clear();
  ClearFlag(ClientFlag_dirty);
}

bool PClient::WaitFor(const PString& key, const PString* target) {
  bool succ = waiting_keys_.insert(key).second;

  if (succ && target) {
    if (!target_.empty()) {
      ERROR("Wait failed for key {}, because old target {}", key, target_);
      waiting_keys_.erase(key);
      return false;
    }

    target_ = *target;
  }

  return succ;
}

void PClient::SetSlaveInfo() { slave_info_.reset(new PSlaveInfo()); }

void PClient::AddCurrentToMonitor() {
  s_monitors_.insert(std::static_pointer_cast<PClient>(s_current->shared_from_this()));
}

void PClient::FeedMonitors(const std::vector<PString>& params) {
  assert(!params.empty());

  if (s_monitors_.empty()) {
    return;
  }

  char buf[512];
  int n = snprintf(buf, sizeof buf, "+[db%d %s:%d]: \"", PSTORE.GetDB(), s_current->tcp_connection_->GetPeerIp().c_str(),
                   s_current->tcp_connection_->GetPeerPort());

  assert(n > 0);

  for (const auto& e : params) {
    if (n < static_cast<int>(sizeof buf)) {
      n += snprintf(buf + n, sizeof buf - n, "%s ", e.data());
    } else {
      break;
    }
  }

  --n;  // no space follow last param

  for (auto it(s_monitors_.begin()); it != s_monitors_.end();) {
    auto m = it->lock();
    if (m) {
      m->tcp_connection_->SendPacketSafely(buf, n);
      m->tcp_connection_->SendPacketSafely("\"" CRLF, 3);

      ++it;
    } else {
      s_monitors_.erase(it++);
    }
  }
}

}  // namespace pikiwidb
