// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include <vector>
#include <algorithm>

#include <glog/logging.h>

#include "slash/include/slash_coding.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_client_conn.h"
#include "include/pika_dispatch_thread.h"
#include "include/pika_cmd_table_manager.h"

extern PikaConf* g_pika_conf;
extern PikaServer* g_pika_server;
extern PikaCmdTableManager* g_pika_cmd_table_manager;

static std::string ConstructPubSubResp(
                                const std::string& cmd,
                                const std::vector<std::pair<std::string, int>>& result) {
  std::stringstream resp;
  if (result.size() == 0) {
    resp << "*3\r\n" << "$" << cmd.length() << "\r\n" << cmd << "\r\n" <<
                        "$" << -1           << "\r\n" << ":" << 0      << "\r\n";
  }
  for (auto it = result.begin(); it != result.end(); it++) {
    resp << "*3\r\n" << "$" << cmd.length()       << "\r\n" << cmd       << "\r\n" <<
                        "$" << it->first.length() << "\r\n" << it->first << "\r\n" <<
                        ":" << it->second         << "\r\n";
  }
  return resp.str();
}

PikaClientConn::PikaClientConn(int fd, std::string ip_port,
                               pink::ServerThread* server_thread,
                               void* worker_specific_data,
                               pink::PinkEpoll* pink_epoll)
      : AsynRedisConn(fd, ip_port, server_thread, pink_epoll),
        server_thread_(server_thread),
        cmds_table_(reinterpret_cast<CmdTable*>(worker_specific_data)),
        is_pubsub_(false) {
  auth_stat_.Init();
}

std::string PikaClientConn::DoCmd(
    PikaCmdArgsType& argv, const std::string& opt) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = g_pika_cmd_table_manager->GetCmd(opt);
  if (!cinfo_ptr || !c_ptr) {
      return "-Err unknown or unsupported command \'" + opt + "\'\r\n";
  }

  // Check authed
  if (!auth_stat_.IsAuthed(cinfo_ptr)) {
    return "-ERR NOAUTH Authentication required.\r\n";
  }

  uint64_t start_us = 0;
  if (g_pika_conf->slowlog_slower_than() >= 0) {
    start_us = slash::NowMicros();
  }

  // For now, only shutdown need check local
  if (cinfo_ptr->is_local()) {
    if (ip_port().find("127.0.0.1") == std::string::npos
        && ip_port().find(g_pika_server->host()) == std::string::npos) {
      LOG(WARNING) << "\'shutdown\' should be localhost";
      return "-ERR \'shutdown\' should be localhost\r\n";
    }
  }

  std::string monitor_message;
  bool is_monitoring = g_pika_server->HasMonitorClients();
  if (is_monitoring) {
    monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) +
      " [0 " + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv.begin(); iter != argv.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  // Initial
  c_ptr->Initial(argv, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return c_ptr->res().message();
  }

  g_pika_server->UpdateQueryNumAndExecCountTable(argv[0]);
 
  // PubSub connection
  if (this->IsPubSub()) {
    if (opt != kCmdNameSubscribe &&
        opt != kCmdNameUnSubscribe &&
        opt != kCmdNamePing &&
        opt != kCmdNamePSubscribe &&
        opt != kCmdNamePUnSubscribe) {
      return "-ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context\r\n";
    }
  }

  // Monitor
  if (opt == kCmdNameMonitor) {
    std::shared_ptr<PinkConn> conn = server_thread_->MoveConnOut(fd());
    assert(conn.get() == this);
    g_pika_server->AddMonitorClient(std::dynamic_pointer_cast<PikaClientConn>(conn));
    g_pika_server->AddMonitorMessage("OK");
    return ""; // Monitor thread will return "OK"
  }

  //PubSub
  if (opt == kCmdNamePSubscribe || opt == kCmdNameSubscribe) {             // PSubscribe or Subscribe
    std::shared_ptr<PinkConn> conn = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
    if (!this->IsPubSub()) {
      conn = server_thread_->MoveConnOut(fd());
    }
    std::vector<std::string > channels;
    for (size_t i = 1; i < argv.size(); i++) {
      channels.push_back(slash::StringToLower(argv[i]));
    }
    std::vector<std::pair<std::string, int>> result;
    g_pika_server->Subscribe(conn, channels, opt == kCmdNamePSubscribe, &result);
    this->SetIsPubSub(true);
    return ConstructPubSubResp(opt, result);
  } else if (opt == kCmdNamePUnSubscribe || opt == kCmdNameUnSubscribe) {  // PUnSubscribe or UnSubscribe
    std::vector<std::string > channels;
    for (size_t i = 1; i < argv.size(); i++) {
      channels.push_back(slash::StringToLower(argv[i]));
    }
    std::vector<std::pair<std::string, int>> result;
    std::shared_ptr<PinkConn> conn = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
    int subscribed = g_pika_server->UnSubscribe(conn, channels, opt == kCmdNamePUnSubscribe, &result);
    if (subscribed == 0 && this->IsPubSub()) {
      /*
       * if the number of client subscribed is zero,
       * the client will exit the Pub/Sub state
       */
      server_thread_->HandleNewConn(fd(), ip_port());
      this->SetIsPubSub(false);
    }
    return ConstructPubSubResp(opt, result);
  }

  if (cinfo_ptr->is_write()) {
    if (g_pika_server->BinlogIoError()) {
      return "-ERR Writing binlog failed, maybe no space left on device\r\n";
    }
    if (g_pika_server->readonly()) {
      return "-ERR Server in read-only\r\n";
    }
    if (argv.size() >= 2) {
      g_pika_server->mutex_record_.Lock(argv[1]);
    }
  }

  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWLockReader();
  }

  uint32_t exec_time = time(nullptr);
  c_ptr->Do();

  if (cinfo_ptr->is_write()
    && g_pika_conf->write_binlog()) {
    if (c_ptr->res().ok()) {
      g_pika_server->logger_->Lock();
      uint32_t filenum = 0;
      uint64_t offset = 0;
      uint64_t logic_id = 0;
      g_pika_server->logger_->GetProducerStatus(&filenum, &offset, &logic_id);

      std::string binlog = c_ptr->ToBinlog(argv,
                                           exec_time,
                                           g_pika_conf->server_id(),
                                           logic_id,
                                           filenum,
                                           offset);
      slash::Status s;
      if (!binlog.empty()) {
        s = g_pika_server->logger_->Put(binlog);
      }

      g_pika_server->logger_->Unlock();
      if (!s.ok()) {
        LOG(WARNING) << "Writing binlog failed, maybe no space left on device";
        g_pika_server->SetBinlogIoError(true);
        return "-ERR Writing binlog failed, maybe no space left on device\r\n";
      }
    }
  }

  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWUnlock();
  }

  if (cinfo_ptr->is_write()) {
    if (argv.size() >= 2) {
      g_pika_server->mutex_record_.Unlock(argv[1]);
    }
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int32_t start_time = start_us / 1000000;
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      g_pika_server->SlowlogPushEntry(argv, start_time, duration);
      if (g_pika_conf->slowlog_write_errorlog()) {
        std::string slow_log;
        for (unsigned int i = 0; i < argv.size(); i++) {
          slow_log.append(" ");
          slow_log.append(slash::ToRead(argv[i]));
          if (slow_log.size() >= 1000) {
            slow_log.resize(1000);
            slow_log.append("...\"");
            break;
          }
        }
        LOG(ERROR) << "ip_port: "<< ip_port() << ", command:" << slow_log << ", start_time(s): " << start_time << ", duration(us): " << duration;
      }
    }
  }

  if (opt == kCmdNameAuth) {
    if (!auth_stat_.ChecknUpdate(c_ptr->res().raw_message())) {
//      LOG(WARNING) << "(" << ip_port() << ")Wrong Password";
    }
  }
  return c_ptr->res().message();
}

void PikaClientConn::AsynProcessRedisCmd() {
  BgTaskArg* arg = new BgTaskArg();
  arg->pcc = std::dynamic_pointer_cast<PikaClientConn>(shared_from_this());
  g_pika_server->Schedule(&DoBackgroundTask, arg);
}

void PikaClientConn::BatchExecRedisCmd() {
  bool success = true;
  for (auto& argv : argvs_) {
    if (DealMessage(argv) != 0) {
      success = false;
      break;
    }
  }
  argvs_.clear();
  NotifyEpoll(success);
}

int PikaClientConn::DealMessage(PikaCmdArgsType& argv) {

  if (argv.empty()) return -2;
  std::string opt = argv[0];
  slash::StringToLower(opt);

  if (response_.empty()) {
    // Avoid memory copy
    response_ = std::move(DoCmd(argv, opt));
  } else {
    // Maybe pipeline
    response_.append(DoCmd(argv, opt));
  }
  return 0;
}

void PikaClientConn::DoBackgroundTask(void* arg) {
  BgTaskArg* bg_arg = reinterpret_cast<BgTaskArg*>(arg);
  bg_arg->pcc->BatchExecRedisCmd();
  delete bg_arg;
}

// Initial permission status
void PikaClientConn::AuthStat::Init() {
  // Check auth required
  stat_ = g_pika_conf->userpass() == "" ?
    kLimitAuthed : kNoAuthed;
  if (stat_ == kLimitAuthed 
      && g_pika_conf->requirepass() == "") {
    stat_ = kAdminAuthed;
  }
}

// Check permission for current command
bool PikaClientConn::AuthStat::IsAuthed(const CmdInfo* const cinfo_ptr) {
  std::string opt = cinfo_ptr->name();
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
      if (cinfo_ptr->is_admin_require() 
          || find(blacklist.begin(), blacklist.end(), opt) != blacklist.end()) {
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
