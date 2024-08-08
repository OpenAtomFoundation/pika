// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <fmt/format.h>
#include <glog/logging.h>
#include <utility>
#include <vector>

#include "include/pika_admin.h"
#include "include/pika_client_conn.h"
#include "include/pika_cmd_table_manager.h"
#include "include/pika_command.h"
#include "include/pika_conf.h"
#include "include/pika_define.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"
#include "net/src/dispatch_thread.h"
#include "net/src/worker_thread.h"
#include "src/pstd/include/scope_record_lock.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

PikaClientConn::PikaClientConn(int fd, const std::string& ip_port, net::Thread* thread, net::NetMultiplexer* mpx,
                               const net::HandleType& handle_type, int max_conn_rbuf_size)
    : RedisConn(fd, ip_port, thread, mpx, handle_type, max_conn_rbuf_size),
      server_thread_(reinterpret_cast<net::ServerThread*>(thread)),
      current_db_(g_pika_conf->default_db()) {
  // client init, set client user is default, and authenticated = false
  UnAuth(g_pika_server->Acl()->GetUserLock(Acl::DefaultUser));
  time_stat_.reset(new TimeStat());
}

std::shared_ptr<Cmd> PikaClientConn::DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr, bool cache_miss_in_rtc) {
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
  c_ptr->SetCacheMissedInRtc(cache_miss_in_rtc);
  c_ptr->SetConn(shared_from_this());
  c_ptr->SetResp(resp_ptr);

  // Check authed
  if (AuthRequired()) {  // the user is not authed, need to do auth
    if (!(c_ptr->flag() & kCmdFlagsNoAuth)) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "NOAUTH Authentication required.");
      return c_ptr;
    }
  }
  // Initial
  c_ptr->Initial(argv, current_db_);
  if (!c_ptr->res().ok()) {
    if (IsInTxn()) {
      SetTxnInitFailState(true);
    }
    return c_ptr;
  }

  int8_t subCmdIndex = -1;
  std::string errKey;
  auto checkRes = user_->CheckUserPermission(c_ptr, argv, subCmdIndex, &errKey);
  std::string cmdName = c_ptr->name();
  if (subCmdIndex >= 0 && checkRes == AclDeniedCmd::CMD) {
    cmdName += "|" + argv[1];
  }

  std::string object;
  switch (checkRes) {
    case AclDeniedCmd::CMD:
      c_ptr->res().SetRes(CmdRes::kNone, fmt::format("-NOPERM this user has no permissions to run the '{}' command\r\n",
                                                     pstd::StringToLower(cmdName)));
      object = cmdName;
      break;
    case AclDeniedCmd::KEY:
      c_ptr->res().SetRes(CmdRes::kNone,
                          "-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
      object = errKey;
      break;
    case AclDeniedCmd::CHANNEL:
      c_ptr->res().SetRes(CmdRes::kNone,
                          "-NOPERM this user has no permissions to access one of the channel used as arguments\r\n");
      object = errKey;
      break;
    case AclDeniedCmd::NO_SUB_CMD:
      c_ptr->res().SetRes(CmdRes::kErrOther, fmt::format("unknown subcommand '{}' subcommand", argv[1]));
      break;
    case AclDeniedCmd::NO_AUTH:
      c_ptr->res().AppendContent("-NOAUTH Authentication required.");
      break;
    default:
      break;
  }

  if (checkRes == AclDeniedCmd::CMD || checkRes == AclDeniedCmd::KEY || checkRes == AclDeniedCmd::CHANNEL) {
    std::string cInfo;
    ClientInfoToString(&cInfo, cmdName);
    int32_t context = IsInTxn() ? static_cast<int32_t>(AclLogCtx::MULTI) : static_cast<int32_t>(AclLogCtx::TOPLEVEL);

    if (checkRes == AclDeniedCmd::CMD && IsInTxn() && cmdName == kCmdNameExec) {
      object = kCmdNameMulti;
    }
    g_pika_server->Acl()->AddLogEntry(static_cast<int32_t>(checkRes), context, user_->Name(), object, cInfo);

    return c_ptr;
  }

  if (IsInTxn() && opt != kCmdNameExec && opt != kCmdNameWatch && opt != kCmdNameDiscard && opt != kCmdNameMulti) {
    if (c_ptr->is_write() && g_pika_server->readonly(current_db_)) {
      SetTxnInitFailState(true);
      c_ptr->res().SetRes(CmdRes::kErrOther, "READONLY You can't write against a read only replica.");
      return c_ptr;
    }
    PushCmdToQue(c_ptr);
    c_ptr->res().SetRes(CmdRes::kTxnQueued);
    return c_ptr;
  }

  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    ProcessMonitor(argv);
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
    if (g_pika_server->readonly(current_db_) && opt != kCmdNameExec) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "READONLY You can't write against a read only replica.");
      return c_ptr;
    }
  } else if (c_ptr->is_read() && c_ptr->flag_ == 0) {
    const auto& server_guard = std::lock_guard(g_pika_server->GetDBLock());
    int role = 0;
    auto status = g_pika_rm->CheckDBRole(current_db_, &role);
    if (!status.ok()) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    } else if ((role & PIKA_ROLE_SLAVE) == PIKA_ROLE_SLAVE) {
      const auto& slave_db = g_pika_rm->GetSyncSlaveDBByName(DBInfo(current_db_));
      if (!slave_db) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
        return c_ptr;
      } else if (slave_db->State() != ReplState::kConnected) {
        c_ptr->res().SetRes(CmdRes::kErrOther, "Full sync not completed");
        return c_ptr;
      }
    }
  }

  if (c_ptr->res().ok() && c_ptr->is_write() && name() != kCmdNameExec) {
    if (c_ptr->name() == kCmdNameFlushdb) {
      auto flushdb = std::dynamic_pointer_cast<FlushdbCmd>(c_ptr);
      SetTxnFailedIfKeyExists(flushdb->GetFlushDBname());
    } else if (c_ptr->name() == kCmdNameFlushall) {
      SetTxnFailedIfKeyExists();
    } else {
      auto table_keys = c_ptr->current_key();
      for (auto& key : table_keys) {
        key = c_ptr->db_name().append("_").append(key);
      }
      SetTxnFailedFromKeys(table_keys);
    }
  }

  // Process Command
  c_ptr->Execute();
  time_stat_->process_done_ts_ = pstd::NowMicros();
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  (*cmdstat_map)[opt].cmd_count.fetch_add(1);
  (*cmdstat_map)[opt].cmd_time_consuming.fetch_add(time_stat_->total_time());

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
                 << ", before_queue_time(ms): " << time_stat_->before_queue_time() / 1000
                 << ", queue_time(ms): " << time_stat_->queue_time() / 1000
                 << ", process_time(ms): " << time_stat_->process_time() / 1000
                 << ", cmd_time(ms): " << do_duration / 1000;
    }
  }
}

void PikaClientConn::ProcessMonitor(const PikaCmdArgsType& argv) {
  std::string monitor_message;
  std::string db_name = current_db_.substr(2);
  monitor_message = std::to_string(1.0 * static_cast<double>(pstd::NowMicros()) / 1000000) + " [" + db_name + " " +
                    this->ip_port() + "]";
  for (const auto& iter : argv) {
    monitor_message += " " + pstd::ToRead(iter);
  }
  g_pika_server->AddMonitorMessage(monitor_message);
}

bool PikaClientConn::IsInterceptedByRTC(std::string& opt) {
  //currently we only Intercept: Get, HGet
  if (opt == kCmdNameGet && g_pika_conf->GetCacheString()) {
    return true;
  }
  if (opt == kCmdNameHGet && g_pika_conf->GetCacheHash()) {
    return true;
  }
  return false;
}

void PikaClientConn::ProcessRedisCmds(const std::vector<net::RedisCmdArgsType>& argvs, bool async,
                                      std::string* response) {
  time_stat_->Reset();
  if (async) {
    auto arg = new BgTaskArg();
    arg->cache_miss_in_rtc_ = false;
    arg->redis_cmds = argvs;
    time_stat_->enqueue_ts_ = time_stat_->before_queue_ts_ = pstd::NowMicros();
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
    bool is_admin_cmd = g_pika_conf->is_admin_cmd(opt);

    //we don't intercept pipeline batch (argvs.size() > 1)
    if (g_pika_conf->rtc_cache_read_enabled() &&
        argvs.size() == 1 && IsInterceptedByRTC(opt) &&
        PIKA_CACHE_NONE != g_pika_conf->cache_mode() &&
        !IsInTxn()) {
      // read in cache
      if (ReadCmdInCache(argvs[0], opt)) {
        delete arg;
        return;
      }
      arg->cache_miss_in_rtc_ = true;
      time_stat_->before_queue_ts_ = pstd::NowMicros();
    }

    g_pika_server->ScheduleClientPool(&DoBackgroundTask, arg, is_slow_cmd, is_admin_cmd);
    return;
  }
  BatchExecRedisCmd(argvs, false);
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

  conn_ptr->BatchExecRedisCmd(bg_arg->redis_cmds, bg_arg->cache_miss_in_rtc_);
}

void PikaClientConn::BatchExecRedisCmd(const std::vector<net::RedisCmdArgsType>& argvs, bool cache_miss_in_rtc) {
  resp_num.store(static_cast<int32_t>(argvs.size()));
  for (const auto& argv : argvs) {
    std::shared_ptr<std::string> resp_ptr = std::make_shared<std::string>();
    resp_array.push_back(resp_ptr);
    ExecRedisCmd(argv, resp_ptr, cache_miss_in_rtc);
  }
  time_stat_->process_done_ts_ = pstd::NowMicros();
  TryWriteResp();
}

bool PikaClientConn::ReadCmdInCache(const net::RedisCmdArgsType& argv, const std::string& opt) {
  resp_num.store(1);
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    return false;
  }
  // Check authed
  if (AuthRequired()) {  // the user is not authed, need to do auth
    if (!(c_ptr->flag() & kCmdFlagsNoAuth)) {
      return false;
    }
  }
  // Initial
  c_ptr->Initial(argv, current_db_);
  //acl check
  int8_t subCmdIndex = -1;
  std::string errKey;
  auto checkRes = user_->CheckUserPermission(c_ptr, argv, subCmdIndex, &errKey);
  std::string object;
  if (checkRes == AclDeniedCmd::CMD ||
      checkRes == AclDeniedCmd::KEY ||
      checkRes == AclDeniedCmd::CHANNEL ||
      checkRes == AclDeniedCmd::NO_SUB_CMD ||
      checkRes == AclDeniedCmd::NO_AUTH
      ) {
    //acl check failed
    return false;
  }
  //only read command(Get, HGet) will reach here, no need of record lock
  bool read_status = c_ptr->DoReadCommandInCache();
  auto cmdstat_map = g_pika_cmd_table_manager->GetCommandStatMap();
  resp_num--;
  if (read_status) {
    time_stat_->process_done_ts_ = pstd::NowMicros();
    (*cmdstat_map)[argv[0]].cmd_count.fetch_add(1);
    (*cmdstat_map)[argv[0]].cmd_time_consuming.fetch_add(time_stat_->total_time());
    resp_array.emplace_back(std::make_shared<std::string>(std::move(c_ptr->res().message())));
    TryWriteResp();
  }
  return read_status;
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

void PikaClientConn::PushCmdToQue(std::shared_ptr<Cmd> cmd) { txn_cmd_que_.push(cmd); }

bool PikaClientConn::IsInTxn() {
  std::lock_guard<std::mutex> lg(txn_state_mu_);
  return txn_state_[TxnStateBitMask::Start];
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

void PikaClientConn::ClearTxnCmdQue() { txn_cmd_que_ = std::queue<std::shared_ptr<Cmd>>{}; }

void PikaClientConn::AddKeysToWatch(const std::vector<std::string>& db_keys) {
  for (const auto& it : db_keys) {
    watched_db_keys_.emplace(it);
  }

  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    dispatcher->AddWatchKeys(watched_db_keys_, shared_from_this());
  }
}

void PikaClientConn::RemoveWatchedKeys() {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    watched_db_keys_.clear();
    dispatcher->RemoveWatchKeys(shared_from_this());
  }
}

void PikaClientConn::SetTxnFailedFromKeys(const std::vector<std::string>& db_keys) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher != nullptr) {
    auto involved_conns = std::vector<std::shared_ptr<NetConn>>{};
    involved_conns = dispatcher->GetInvolvedTxn(db_keys);
    for (auto& conn : involved_conns) {
      if (auto c = std::dynamic_pointer_cast<PikaClientConn>(conn); c != nullptr) {
        c->SetTxnWatchFailState(true);
      }
    }
  }
}

// if key in target_db exists, then the key been watched multi will be failed
void PikaClientConn::SetTxnFailedIfKeyExists(std::string target_db_name) {
  auto dispatcher = dynamic_cast<net::DispatchThread*>(server_thread());
  if (dispatcher == nullptr) {
    return;
  }
  auto involved_conns = dispatcher->GetAllTxns();
  for (auto& conn : involved_conns) {
    std::shared_ptr<PikaClientConn> c;
    if (c = std::dynamic_pointer_cast<PikaClientConn>(conn); c == nullptr) {
      continue;
    }

    for (const auto& db_key : c->watched_db_keys_) {
      size_t pos = db_key.find('_');
      if (pos == std::string::npos) {
        continue;
      }

      auto db_name = db_key.substr(0, pos);
      auto key = db_key.substr(pos + 1);

      if (target_db_name == "" || target_db_name == "all" || target_db_name == db_name) {
        auto db = g_pika_server->GetDB(db_name);
        // if watched key exists, set watch state to failed
        if (db->storage()->Exists({key}) > 0) {
          c->SetTxnWatchFailState(true);
          break;
        }
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

void PikaClientConn::ExecRedisCmd(const PikaCmdArgsType& argv, std::shared_ptr<std::string>& resp_ptr,
                                  bool cache_miss_in_rtc) {
  // get opt
  std::string opt = argv[0];
  pstd::StringToLower(opt);
  if (opt == kClusterPrefix) {
    if (argv.size() >= 2) {
      opt += argv[1];
      pstd::StringToLower(opt);
    }
  }

  std::shared_ptr<Cmd> cmd_ptr = DoCmd(argv, opt, resp_ptr, cache_miss_in_rtc);
  *resp_ptr = std::move(cmd_ptr->res().message());
  resp_num--;
}

std::queue<std::shared_ptr<Cmd>> PikaClientConn::GetTxnCmdQue() { return txn_cmd_que_; }

void PikaClientConn::DoAuth(const std::shared_ptr<User>& user) {
  user_ = user;
  authenticated_ = true;
}

void PikaClientConn::UnAuth(const std::shared_ptr<User>& user) {
  user_ = user;
  // If the user does not have a password, and the user is valid, then the user does not need authentication
  authenticated_ = user_->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS)) &&
                   !user_->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED));
}

bool PikaClientConn::IsAuthed() const { return authenticated_; }

bool PikaClientConn::AuthRequired() const {
  // If the user does not have a password, and the user is valid, then the user does not need authentication
  // Otherwise, you need to determine whether go has been authenticated
  return (!user_->HasFlags(static_cast<uint32_t>(AclUserFlag::NO_PASS)) ||
          user_->HasFlags(static_cast<uint32_t>(AclUserFlag::DISABLED))) &&
         !IsAuthed();
}
std::string PikaClientConn::UserName() const { return user_->Name(); }

void PikaClientConn::ClientInfoToString(std::string* info, const std::string& cmdName) {
  uint64_t age = pstd::NowMicros() - last_interaction().tv_usec;

  std::string flags;
  g_pika_server->ClientIsMonitor(std::dynamic_pointer_cast<PikaClientConn>(shared_from_this())) ? flags.append("O")
                                                                                                : flags.append("S");
  if (IsPubSub()) {
    flags.append("P");
  }

  info->append(fmt::format(
      "id={} addr={} name={} age={} idle={} flags={} db={} sub={} psub={} multi={} "
      "cmd={} user={} resp=2",
      fd(), ip_port(), name(), age, age / 1000000, flags, GetCurrentTable(),
      IsPubSub() ? g_pika_server->ClientPubSubChannelSize(shared_from_this()) : 0,
      IsPubSub() ? g_pika_server->ClientPubSubChannelPatternSize(shared_from_this()) : 0, -1, cmdName, user_->Name()));
}

// compare addr in ClientInfo
bool AddrCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return rhs.ip_port < lhs.ip_port; }

bool IdleCompare(const ClientInfo& lhs, const ClientInfo& rhs) { return lhs.last_interaction < rhs.last_interaction; }
