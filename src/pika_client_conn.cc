// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_client_conn.h"

#include <algorithm>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "include/pika_admin.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "net/src/dispatch_thread.h"
#include "net/src/worker_thread.h"

extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;



PikaClientConn::PikaClientConn(int fd, const std::string& ip_port, net::Thread* thread, net::NetMultiplexer* mpx,
                               const net::HandleType& handle_type, int max_conn_rbuf_size)
    : RedisConn(fd, ip_port, thread, mpx, handle_type, max_conn_rbuf_size),
      server_thread_(reinterpret_cast<net::ServerThread*>(thread)),
      current_db_(g_pika_conf->default_db()) {
  auth_stat_.Init();
  time_stat_.reset(new TimeStat());
}

std::shared_ptr<Cmd> PikaClientConn::DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr) {
  // Get command info
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    std::shared_ptr<Cmd> tmp_ptr = std::make_shared<DummyCmd>(DummyCmd());
    tmp_ptr->res().SetRes(CmdRes::kErrOther, "unknown command \"" + opt + "\"");
    if (IsInTxn()) {
      SetTxnInitFailState(true);
    }
    return tmp_ptr;
  }
  c_ptr->SetConn(shared_from_this());
  c_ptr->SetResp(resp_ptr);

  // Check authed
  // AuthCmd will set stat_
  if (!auth_stat_.IsAuthed(c_ptr)) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "NOAUTH Authentication required.");
    return c_ptr;
  }

  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    ProcessMonitor(argv);
  }

  // Initial
  c_ptr->Initial(argv, current_db_);
  if (!c_ptr->res().ok()) {
    if (IsInTxn()) {
      SetTxnInitFailState(true);
    }
    return c_ptr;
  }
  if (IsInTxn() && opt != kCmdNameExec && opt != kCmdNameWatch && opt != kCmdNameDiscard && opt != kCmdNameMulti) {
    PushCmdToQue(c_ptr);
    c_ptr->res().SetRes(CmdRes::kTxnQueued);
    return c_ptr;
  }
  g_pika_server->UpdateQueryNumAndExecCountDB(current_db_, opt, c_ptr->is_write());

  // PubSub connection
  // (P)SubscribeCmd will set is_pubsub_
  if (this->IsPubSub()) {
    if (opt != kCmdNameSubscribe && opt != kCmdNameUnSubscribe && opt != kCmdNamePing && opt != kCmdNamePSubscribe &&
        opt != kCmdNamePUnSubscribe) {
      c_ptr->res().SetRes(CmdRes::kErrOther,
                          "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
      return c_ptr;
    }
  }

  // reject all the request before new master sync finished
  if (g_pika_server->leader_protected_mode()) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "Cannot process command before new leader sync finished");
    return c_ptr;
  }

  if (!g_pika_server->IsDBExist(current_db_)) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "DB not found");
    return c_ptr;
  }

  if (c_ptr->is_write()) {
    if (g_pika_server->IsDBBinlogIoError(current_db_)) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Writing binlog failed, maybe no space left on device");
      return c_ptr;
    }
    std::vector<std::string> cur_key = c_ptr->current_key();
    if (cur_key.empty() && opt != kCmdNameExec) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    }
    if (g_pika_server->readonly(current_db_, cur_key.front())) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Server in read-only");
      return c_ptr;
    }
  }

  // Process Command
  c_ptr->Execute();
  time_stat_->process_done_ts_ = pstd::NowMicros();
  auto cmdstat_map = g_pika_server->GetCommandStatMap();
  (*cmdstat_map)[opt].cmd_count.fetch_add(1);
  (*cmdstat_map)[opt].cmd_time_consuming.fetch_add(time_stat_->total_time());

  if (c_ptr->res().ok() && c_ptr->is_write() && name() != kCmdNameExec) {
    if (c_ptr->name() == kCmdNameFlushdb) {
      auto flushdb = std::dynamic_pointer_cast<FlushdbCmd>(c_ptr);
      SetTxnFailedFromDBs(flushdb->GetFlushDname());
    } else if (c_ptr->name() == kCmdNameFlushall) {
      SetAllTxnFailed();
    } else {
      auto table_keys = c_ptr->current_key();
      for (auto& key : table_keys) {
        key = c_ptr->db_name().append(key);
      }
      SetTxnFailedFromKeys(table_keys);
    }
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    ProcessSlowlog(argv, c_ptr->GetDoDuration());
  }

  return c_ptr;
}

void PikaClientConn::ProcessSlowlog(const PikaCmdArgsType& argv, uint64_t do_duration) {
  if (time_stat_->total_time() > g_pika_conf->slowlog_slower_than()) {
    g_pika_server->SlowlogPushEntry(argv, time_stat_->start_ts() / 1000000, time_stat_->total_time());
    if (g_pika_conf->slowlog_write_errorlog()) {
      bool trim = false;
      std::string slow_log;
      uint32_t cmd_size = 0;
      for (const auto& i : argv) {
        cmd_size += 1 + i.size();  // blank space and argument length
        if (!trim) {
          slow_log.append(" ");
          slow_log.append(pstd::ToRead(i));
          if (slow_log.size() >= 1000) {
            trim = true;
            slow_log.resize(1000);
            slow_log.append("...\"");
          }
        }
      }
      LOG(ERROR) << "ip_port: " << ip_port() << ", db: " << current_db_ << ", command:" << slow_log
                 << ", command_size: " << cmd_size - 1 << ", arguments: " << argv.size()
                 << ", total_time(ms): " << time_stat_->total_time() / 1000
                 << ", queue_time(ms): " << time_stat_->queue_time() / 1000
                 << ", process_time(ms): " << time_stat_->process_time() / 1000
                 << ", cmd_time(ms): " << do_duration / 1000;
    }
  }
}

void PikaClientConn::ProcessMonitor(const PikaCmdArgsType& argv) {
  std::string monitor_message;
  std::string db_name = current_db_.substr(2);
  monitor_message = std::to_string(1.0 * static_cast<double>(pstd::NowMicros()) / 1000000) + " [" + db_name + " " + this->ip_port() + "]";
  for (const auto& iter : argv) {
    monitor_message += " " + pstd::ToRead(iter);
  }
  g_pika_server->AddMonitorMessage(monitor_message);
}

void PikaClientConn::ProcessRedisCmds(const std::vector<net::RedisCmdArgsType>& argvs, bool async,
                                      std::string* response) {
  time_stat_->Reset();
  if (async) {
    auto arg = new BgTaskArg();
    arg->redis_cmds = argvs;
    time_stat_->enqueue_ts_ = pstd::NowMicros();
    arg->conn_ptr = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
    /**
     * If using the pipeline method to transmit batch commands to Pika, it is unable to
     * correctly distinguish between fast and slow commands.
     * However, if using the pipeline method for Codis, it can correctly distinguish between
     * fast and slow commands, but it cannot guarantee sequential execution.
     */
    std::string opt = argvs[0][0];
    pstd::StringToLower(opt);
    bool is_slow_cmd = g_pika_conf->is_slow_cmd(opt);
    g_pika_server->ScheduleClientPool(&DoBackgroundTask, arg, is_slow_cmd);
    return;
  }
  BatchExecRedisCmd(argvs);
}

void PikaClientConn::DoBackgroundTask(void* arg) {
  std::unique_ptr<BgTaskArg> bg_arg(static_cast<BgTaskArg*>(arg));
  std::shared_ptr<PikaClientConn> conn_ptr = bg_arg->conn_ptr;
  conn_ptr->time_stat_->dequeue_ts_ = pstd::NowMicros();
  if (bg_arg->redis_cmds.empty()) {
    conn_ptr->NotifyEpoll(false);
    return;
  }
  for (const auto& argv : bg_arg->redis_cmds) {
    if (argv.empty()) {
      conn_ptr->NotifyEpoll(false);
      return;
    }
  }

  conn_ptr->BatchExecRedisCmd(bg_arg->redis_cmds);
}

void PikaClientConn::DoExecTask(void* arg) {
  std::unique_ptr<BgTaskArg> bg_arg(static_cast<BgTaskArg*>(arg));
  std::shared_ptr<Cmd> cmd_ptr = bg_arg->cmd_ptr;
  std::shared_ptr<PikaClientConn> conn_ptr = bg_arg->conn_ptr;
  std::shared_ptr<std::string> resp_ptr = bg_arg->resp_ptr;
  LogOffset offset = bg_arg->offset;
  std::string db_name = bg_arg->db_name;
  uint32_t slot_id = bg_arg->slot_id;
  bg_arg.reset();

  cmd_ptr->SetStage(Cmd::kExecuteStage);
  cmd_ptr->Execute();
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    conn_ptr->ProcessSlowlog(cmd_ptr->argv(), cmd_ptr->GetDoDuration());
  }

  std::shared_ptr<SyncMasterSlot> slot = g_pika_rm->GetSyncMasterSlotByName(SlotInfo(db_name, slot_id));
  if (!slot) {
    LOG(WARNING) << "Sync Master Slot not exist " << db_name << slot_id;
    return;
  }
  slot->ConsensusUpdateAppliedIndex(offset);

  if (!conn_ptr || !resp_ptr) {
    return;
  }

  *resp_ptr = std::move(cmd_ptr->res().message());
  // last step to update resp_num, early update may casue another therad may
  // TryWriteResp success with resp_ptr not updated
  conn_ptr->resp_num--;
  conn_ptr->TryWriteResp();
}

void PikaClientConn::BatchExecRedisCmd(const std::vector<net::RedisCmdArgsType>& argvs) {
  resp_num.store(static_cast<int32_t>(argvs.size()));
  for (const auto& argv : argvs) {
    std::shared_ptr<std::string> resp_ptr = std::make_shared<std::string>();
    resp_array.push_back(resp_ptr);
    ExecRedisCmd(argv, resp_ptr);
  }
  time_stat_->process_done_ts_ = pstd::NowMicros();
  TryWriteResp();
}

void PikaClientConn::TryWriteResp() {
  int expected = 0;
  if (resp_num.compare_exchange_strong(expected, -1)) {
    for (auto& resp : resp_array) {
      WriteResp(*resp);
    }
    if (write_completed_cb_) {
      write_completed_cb_();
      write_completed_cb_ = nullptr;
    }
    resp_array.clear();
    NotifyEpoll(true);
  }
}

void PikaClientConn::PushCmdToQue(std::shared_ptr<Cmd> cmd) {
  txn_cmd_que_.push(cmd);
}

bool PikaClientConn::IsInTxn() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Start];
}

bool PikaClientConn::IsTxnFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::WatchFailed] | txn_state_[TxnStateBitMask::InitCmdFailed];
}

bool PikaClientConn::IsTxnInitFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::InitCmdFailed];
}

bool PikaClientConn::IsTxnWatchFailed() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::WatchFailed];
}

bool PikaClientConn::IsTxnExecing() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Execing] && txn_state_[TxnStateBitMask::Start];
}

void PikaClientConn::SetTxnWatchFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::WatchFailed] = is_failed;
}

void PikaClientConn::SetTxnInitFailState(bool is_failed) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::InitCmdFailed] = is_failed;
}

void PikaClientConn::SetTxnStartState(bool is_start) {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  txn_state_[TxnStateBitMask::Start] = is_start;
}

void PikaClientConn::ClearTxnCmdQue() {
  txn_cmd_que_ = std::queue<std::shared_ptr<Cmd>>{};
}


void PikaClientConn::AddKeysToWatch(const std::vector<std::string> &db_keys) {
  for (const auto &it : db_keys) {
    watched_db_keys_.emplace(it);
  }

  auto dispatcher = dynamic_cast<net::DispatchThread *>(server_thread());
  if (dispatcher != nullptr) {
    dispatcher->AddWatchKeys(watched_db_keys_, shared_from_this());
  }
}

void PikaClientConn::RemoveWatchedKeys() {
  auto dispatcher = dynamic_cast<net::DispatchThread *>(server_thread());
  if (dispatcher != nullptr) {
    watched_db_keys_.clear();
    dispatcher->RemoveWatchKeys(shared_from_this());
  }
}

void PikaClientConn::SetTxnFailedFromKeys(const std::vector<std::string> &db_keys) {
  auto dispatcher = dynamic_cast<net::DispatchThread *>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
    involved_conns = dispatcher->GetInvolvedTxn(db_keys);
    for (auto &conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr && c.get() != this) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

void PikaClientConn::SetAllTxnFailed() {
  auto dispatcher = dynamic_cast<net::DispatchThread *>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = dispatcher->GetAllTxns();
    for (auto &conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr && c.get() != this) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

void PikaClientConn::SetTxnFailedFromDBs(std::string db_name) {
  auto dispatcher = dynamic_cast<net::DispatchThread *>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = dispatcher->GetDBTxns(db_name);
    for (auto &conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr && c.get() != this) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

void PikaClientConn::ExitTxn() {
  if (IsInTxn()) {
    RemoveWatchedKeys();
    ClearTxnCmdQue();
    std::lock_guard<std::mutex> lg(txn_state_mu_);
    txn_state_.reset();
  }
}

void PikaClientConn::ExecRedisCmd(const PikaCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr) {
  // get opt
  std::string opt = argv[0];
  pstd::StringToLower(opt);
  if (opt == kClusterPrefix) {
    if (argv.size() >= 2) {
      opt += argv[1];
      pstd::StringToLower(opt);
    }
  }

  std::shared_ptr<Cmd> cmd_ptr = DoCmd(argv, opt, resp_ptr);
  *resp_ptr = std::move(cmd_ptr->res().message());
  resp_num--;
}

std::queue<std::shared_ptr<Cmd>> PikaClientConn::GetTxnCmdQue() {
  return txn_cmd_que_;
}

// Initial permission status
void PikaClientConn::AuthStat::Init() {
  // Check auth required
  stat_ = g_pika_conf->userpass().empty() ? kLimitAuthed : kNoAuthed;
  if (stat_ == kLimitAuthed && g_pika_conf->requirepass().empty()) {
    stat_ = kAdminAuthed;
  }
}

// Check permission for current command
bool PikaClientConn::AuthStat::IsAuthed(const std::shared_ptr<Cmd>& cmd_ptr) {
  std::string opt = cmd_ptr->name();
  if (opt == kCmdNameAuth) {
    return true;
  }
  const std::vector<std::string>& blacklist = g_pika_conf->vuser_blacklist();
  switch (stat_) {
    case kNoAuthed:
      return false;
    case kAdminAuthed:
      break;
    case kLimitAuthed:
      if (cmd_ptr->IsAdminRequire() || find(blacklist.begin(), blacklist.end(), opt) != blacklist.end()) {
        return false;
      }
      break;
    default:
      LOG(WARNING) << "Invalid auth stat : " << static_cast<unsigned>(stat_);
      return false;
  }
  return true;
}

// Update permission status
bool PikaClientConn::AuthStat::ChecknUpdate(const std::string& message) {
  // Situations to change auth status
  if (message == "USER") {
    stat_ = kLimitAuthed;
  } else if (message == "ROOT") {
    stat_ = kAdminAuthed;
  } else {
    return false;
  }
  return true;
}

// compare addr in ClientInfo
bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return rhs.ip_port < lhs.ip_port; }

bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return lhs.last_interaction < rhs.last_interaction; }
