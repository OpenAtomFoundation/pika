// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <sstream>
#include <vector>
#include <algorithm>

#include <glog/logging.h>

#include "slash/include/slash_coding.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_client_conn.h"
#include "pika_dispatch_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;
static const int RAW_ARGS_LEN = 1024 * 1024; 

PikaClientConn::PikaClientConn(int fd, std::string ip_port,
                               pink::ServerThread* server_thread,
                               void* worker_specific_data)
      : RedisConn(fd, ip_port, server_thread),
        server_thread_(server_thread),
        cmds_table_(reinterpret_cast<CmdTable*>(worker_specific_data)) {
  auth_stat_.Init();
}

std::string PikaClientConn::RestoreArgs() {
  std::string res;
  res.reserve(RAW_ARGS_LEN);
  RedisAppendLen(res, argv_.size(), "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    RedisAppendLen(res, (*it).size(), "$");
    RedisAppendContent(res, *it);
  }
  return res;
}

std::string PikaClientConn::DoCmd(const std::string& opt) {
  // Get command info
  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = GetCmdFromTable(opt, *cmds_table_);
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
      " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  // Initial
  c_ptr->Initial(argv_, cinfo_ptr);
  if (!c_ptr->res().ok()) {
    return c_ptr->res().message();
  }

  if (opt == kCmdNameMonitor) {
    pink::PinkConn* conn = server_thread_->MoveConnOut(fd());
    assert(conn == this);
    g_pika_server->AddMonitorClient(static_cast<PikaClientConn*>(conn));
    g_pika_server->AddMonitorMessage("OK");
    return ""; // Monitor thread will return "OK"
  }

  // std::string raw_args;
  std::string need_send_to_hub = "0" /* or "1" */;
  if (cinfo_ptr->is_write()) {
    if (g_pika_conf->readonly()) {
      return "-ERR Server in read-only\r\n";
    }
    // raw_args = RestoreArgs();
    if (argv_.size() >= 2) {
      if (argv_[1] == kCmdNameSet) {
        need_send_to_hub = "1";
      }
      g_pika_server->mutex_record_.Lock(argv_[1]);
    }
  }

  // Add read lock for no suspend command
  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWLockReader();
  }

  uint32_t exec_time = time(nullptr);
  c_ptr->Do();

  if (cinfo_ptr->is_write()) {
    if (c_ptr->res().ok()) {
      g_pika_server->logger_->Lock();

      uint32_t filenum = 0;
      uint64_t offset = 0;
      std::string binlog_info;
      g_pika_server->logger_->GetProducerStatus(&filenum, &offset);
      slash::PutFixed32(&binlog_info, exec_time);
      slash::PutFixed32(&binlog_info, filenum);
      slash::PutFixed64(&binlog_info, offset);

      argv_.push_back(kPikaBinlogMagic);
      argv_.push_back(g_pika_conf->server_id());
      argv_.push_back(binlog_info);
      argv_.push_back(need_send_to_hub);

      g_pika_server->logger_->Put(RestoreArgs());
      g_pika_server->logger_->Unlock();
    }
  }

  if (!cinfo_ptr->is_suspend()) {
    g_pika_server->RWUnlock();
  }

  if (cinfo_ptr->is_write()) {
    if (argv_.size() >= 2) {
      g_pika_server->mutex_record_.Unlock(argv_[1]);
    }
  }

  if (g_pika_conf->slowlog_slower_than() >= 0) {
    int64_t duration = slash::NowMicros() - start_us;
    if (duration > g_pika_conf->slowlog_slower_than()) {
      std::string slow_log;
      for (unsigned int i = 0; i < argv_.size(); i++) {
        slow_log.append(" ");
        slow_log.append(slash::ToRead(argv_[i]));
        if (slow_log.size() >= 1000) {
          slow_log.resize(1000);
          slow_log.append("...\"");
          break;
        }
      }
      LOG(ERROR) << "command:" << slow_log << ", start_time(s): " << start_us / 1000000 << ", duration(us): " << duration;
    }
  }

  if (opt == kCmdNameAuth) {
    if(!auth_stat_.ChecknUpdate(c_ptr->res().raw_message())) {
//      LOG(WARNING) << "(" << ip_port() << ")Wrong Password";
    }
  }
  return c_ptr->res().message();
}

int PikaClientConn::DealMessage() {
  g_pika_server->PlusThreadQuerynum();
  
  if (argv_.empty()) return -2;
  std::string opt = argv_[0];
  slash::StringToLower(opt);
  std::string res = DoCmd(opt);
  
  while ((wbuf_size_ - wbuf_len_ <= res.size())) {
    if (!ExpandWbuf()) {
      LOG(WARNING) << "wbuf is too large";
      memcpy(wbuf_, "-ERR buf is too large\r\n", 23);
      wbuf_len_ = 23;
      set_is_reply(true);
      return 0;
    }
  }
  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
  wbuf_len_ += res.size();
  set_is_reply(true);
  return 0;
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
  } else if (message == "ROOT"){
    stat_ = kAdminAuthed;
  } else {
    return false;
  }
  return true;
}
