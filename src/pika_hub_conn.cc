// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "slash/include/slash_string.h"
#include "slash/include/slash_coding.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"
#include "include/pika_hub_manager.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaHubConn::PikaHubConn(int fd, std::string ip_port, CmdTable* cmds)
    : RedisConn(fd, ip_port, nullptr),
      cmds_(cmds) {
}

int PikaHubConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);
  g_pika_server->PlusThreadQuerynum();
  if (argv_.empty()) {
    return -2;
  }

  // Monitor related
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0*slash::NowMicros()/1000000)
      + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  std::string opt = slash::StringToLower(argv_[0]);

  if (opt == "ping") {
    set_is_reply(true);
    memcpy(wbuf_ + wbuf_len_, "+PONG\r\n", 7);
    wbuf_len_ += 7;
    DLOG(INFO) << "Receive ping";
    return 0;
  }

  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = GetCmdFromTable(opt, *cmds_);
  std::string dummy_binlog_info("X");
  if (!cinfo_ptr || !c_ptr || !(cinfo_ptr->flag_type() & kCmdFlagsKv)) {
    // Error message
    set_is_reply(true);
    memcpy(wbuf_ + wbuf_len_, "-Err Fuck youself\r\n", 19);
    wbuf_len_ += 19;
    return 0;
  }

  g_pika_server->logger_->Lock();
  g_pika_server->logger_->Put(c_ptr->ToBinlog(
      argv_,
      g_pika_conf->server_id(),
      dummy_binlog_info,
      false /* need not send to hub */));
  g_pika_server->logger_->Unlock();

  PikaCmdArgsType *argv = new PikaCmdArgsType(argv_);
  std::string dispatch_key = argv_.size() >= 2 ? argv_[1] : argv_[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, argv,
      0 /* Unused */, true /* Set hub connection's bgworker readonly */);
  return 0;
}
