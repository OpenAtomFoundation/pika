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
#include "include/pika_conf.h"
#include "include/pika_rm.h"
#include "include/pika_server.h"

extern std::unique_ptr<PikaConf> g_pika_conf;
extern PikaServer* g_pika_server;
extern std::unique_ptr<PikaReplicaManager> g_pika_rm;
extern std::unique_ptr<PikaCmdTableManager> g_pika_cmd_table_manager;

PikaClientConn::PikaClientConn(int fd, const std::string& ip_port, net::Thread* thread, net::NetMultiplexer* mpx,
                               const net::HandleType& handle_type, int max_conn_rbuf_size)
    : RedisConn(fd, ip_port, thread, mpx, handle_type, max_conn_rbuf_size),
      server_thread_(reinterpret_cast<net::ServerThread*>(thread)),
      current_db_(g_pika_conf->default_db()) {
  auth_stat_.Init();
}

std::shared_ptr<Cmd> PikaClientConn::DoCmd(const PikaCmdArgsType& argv, const std::string& opt,
                                           const std::shared_ptr<std::string>& resp_ptr) {
  // Get command info
  std::shared_ptr<Cmd> c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!c_ptr) {
    std::shared_ptr<Cmd> tmp_ptr = std::make_shared<DummyCmd>(DummyCmd());
    tmp_ptr->res().SetRes(CmdRes::kErrOther, "unknown command \"" + opt + "\"");
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

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = pstd::NowMicros();
  }

  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    ProcessMonitor(argv);
  }

  // Initial
  c_ptr->Initial(argv, current_db_);
  if (!c_ptr->res().ok()) {
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

  if (g_pika_conf->consensus_level() != 0 && c_ptr->is_write()) {
    c_ptr->SetStage(Cmd::kBinlogStage);
  }
  if (!g_pika_server->IsCommandSupport(opt)) {
    c_ptr->res().SetRes(CmdRes::kErrOther, "This command is not supported in current configuration");
    return c_ptr;
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
    if (cur_key.empty()) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Internal ERROR");
      return c_ptr;
    }
    if (g_pika_server->readonly(current_db_, cur_key.front())) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Server in read-only");
      return c_ptr;
    }
    if (!g_pika_server->ConsensusCheck(current_db_, cur_key.front())) {
      c_ptr->res().SetRes(CmdRes::kErrOther, "Consensus level not match");
    }
  }

  // Process Command
  c_ptr->Execute();
  auto duration = static_cast<int64_t>(pstd::NowMicros() - start_us);
  auto iter = g_pika_cmd_table_manager->GetCmdTable();
  (*iter)[opt]->state.cmd_count.fetch_add(1);
  (*iter)[opt]->state.cmd_time_consuming.fetch_add(static_cast<int32_t>(duration));

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    ProcessSlowlog(argv, start_us, c_ptr->GetDoDuration());
  }
  if (g_pika_conf->consensus_level() != 0 && c_ptr->is_write()) {
    c_ptr->SetStage(Cmd::kExecuteStage);
  }

  return c_ptr;
}

void PikaClientConn::ProcessSlowlog(const PikaCmdArgsType& argv, uint64_t start_us, uint64_t do_duration) {
  auto start_time = static_cast<int32_t>(start_us / 1000000);
  auto duration = static_cast<int64_t>(pstd::NowMicros() - start_us);
  if (duration > g_pika_conf->slowlog_slower_than()) {
    g_pika_server->SlowlogPushEntry(argv, start_time, duration);
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
                 << ", start_time(s): " << start_time << ", duration(us): " << duration
                 << ", do_duration_(us): " << do_duration;
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
  if (async) {
    auto arg = new BgTaskArg();
    arg->redis_cmds = argvs;
    arg->conn_ptr = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
    g_pika_server->ScheduleClientPool(&DoBackgroundTask, arg);
    return;
  }
  BatchExecRedisCmd(argvs);
}

void PikaClientConn::DoBackgroundTask(void* arg) {
  std::unique_ptr<BgTaskArg> bg_arg(static_cast<BgTaskArg*>(arg));
  std::shared_ptr<PikaClientConn> conn_ptr = bg_arg->conn_ptr;
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

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = pstd::NowMicros();
  }
  cmd_ptr->SetStage(Cmd::kExecuteStage);
  cmd_ptr->Execute();
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    conn_ptr->ProcessSlowlog(cmd_ptr->argv(), start_us, cmd_ptr->GetDoDuration());
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

void PikaClientConn::ExecRedisCmd(const PikaCmdArgsType& argv, const std::shared_ptr<std::string>& resp_ptr) {
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
  // level == 0 or (cmd error) or (is_read)
  if (g_pika_conf->consensus_level() == 0 || !cmd_ptr->res().ok() || !cmd_ptr->is_write()) {
    *resp_ptr = std::move(cmd_ptr->res().message());
    resp_num--;
  }
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
      if (cmd_ptr->is_admin_require() || find(blacklist.begin(), blacklist.end(), opt) != blacklist.end()) {
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
