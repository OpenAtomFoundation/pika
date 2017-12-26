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

int PikaHubConn::DealMessage(
    PikaCmdArgsType& argv, std::string* response) {
  //no reply
  //eq set_is_reply(false);
  g_pika_server->PlusThreadQuerynum();
  if (argv.empty()) {
    return -2;
  }

  // Monitor related
  if (g_pika_server->HasMonitorClients()) {
    std::string monitor_message = std::to_string(1.0*slash::NowMicros()/1000000)
      + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv.begin(); iter != argv.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->AddMonitorMessage(monitor_message);
  }

  std::string opt = slash::StringToLower(argv[0]);

  if (opt == "ping") {
    response->append("+PONG\r\n");
    return 0;
  }

  const CmdInfo* const cinfo_ptr = GetCmdInfo(opt);
  Cmd* c_ptr = GetCmdFromTable(opt, *cmds_);
  std::string dummy_binlog_info("X");
  if (!cinfo_ptr || !c_ptr || !(cinfo_ptr->flag_type() & kCmdFlagsKv)) {
    // Error message
    response->append("-Err Fuck youself\r\n");
    return 0;
  }

  g_pika_server->logger_->Lock();
  g_pika_server->logger_->Put(c_ptr->ToBinlog(
      argv,
      g_pika_conf->server_id(),
      dummy_binlog_info,
      false /* need not send to hub */));
  g_pika_server->logger_->Unlock();

  PikaCmdArgsType *v = new PikaCmdArgsType(argv);
  std::string dispatch_key = argv.size() >= 2 ? argv[1] : argv[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, v,
      0 /* Unused */, true /* Set hub connection's bgworker readonly */);
  return 0;
}
