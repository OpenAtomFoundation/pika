// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "slash/include/slash_coding.h"
#include <glog/logging.h>
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_hub_receiver_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaHubConn::PikaHubConn(int fd, std::string ip_port,
                               void* worker_specific_data)
      : RedisConn(fd, ip_port, NULL) {
  hub_receiver_ =
    reinterpret_cast<PikaHubReceiverThread*>(worker_specific_data);
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

  uint64_t serial = hub_receiver_->GetnPlusSerial();

  PikaCmdArgsType *argv = new PikaCmdArgsType(argv_);
  std::string dispatch_key = argv_.size() >= 2 ? argv_[1] : argv_[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, argv,
      serial, g_pika_conf->readonly());
  return 0;
}
