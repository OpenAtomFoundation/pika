// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_conf.h"
#include "pika_binlog_receiver_thread.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

static const int RAW_ARGS_LEN = 1024 * 1024; 
PikaMasterConn::PikaMasterConn(int fd, std::string ip_port, pink::Thread* thread) :
  RedisConn(fd, ip_port) {
//  self_thread_ = reinterpret_cast<PikaBinlogReceiverThread*>(thread);
  raw_args_.reserve(RAW_ARGS_LEN);
  self_thread_ = dynamic_cast<PikaBinlogReceiverThread*>(thread);
}

PikaMasterConn::~PikaMasterConn() {
}

void PikaMasterConn::RestoreArgs() {
  raw_args_.clear();
  RedisAppendLen(raw_args_, argv_.size(), "*");
  PikaCmdArgsType::const_iterator it = argv_.begin();
  for ( ; it != argv_.end(); ++it) {
    RedisAppendLen(raw_args_, (*it).size(), "$");
    RedisAppendContent(raw_args_, *it);
  }
}

int PikaMasterConn::DealMessage() {
  //no reply
  //eq set_is_reply(false);
  self_thread_ -> PlusThreadQuerynum();
  if (argv_.empty()) {
    return -2;
  }

  // Monitor related
  std::string monitor_message;
  bool is_monitoring = g_pika_server->monitor_thread()->HasMonitorClients();
  if (is_monitoring) {
    monitor_message = std::to_string(1.0*slash::NowMicros()/1000000) + " [" + this->ip_port() + "]";
    for (PikaCmdArgsType::iterator iter = argv_.begin(); iter != argv_.end(); iter++) {
      monitor_message += " " + slash::ToRead(*iter);
    }
    g_pika_server->monitor_thread()->AddMonitorMessage(monitor_message);
  }
  RestoreArgs();

  bool is_readonly = g_pika_conf->readonly();

  // Here, the binlog dispatch thread, instead of the binlog bgthread takes on the task to write binlog
  // Only when the server is readonly
  uint64_t serial = self_thread_->GetnPlusSerial();
  if (is_readonly) {
    if (!g_pika_server->WaitTillBinlogBGSerial(serial)) {
      return -2;
    }
    g_pika_server->logger_->Lock();
    g_pika_server->logger_->Put(raw_args_);
    g_pika_server->logger_->Unlock();
    g_pika_server->SignalNextBinlogBGSerial();
  }

  PikaCmdArgsType *argv = new PikaCmdArgsType(argv_);
  std::string dispatch_key = argv_.size() >= 2 ? argv_[1] : argv_[0];
  g_pika_server->DispatchBinlogBG(dispatch_key, argv, raw_args_,
      serial, is_readonly);
  //  memcpy(wbuf_ + wbuf_len_, res.data(), res.size());
  //  wbuf_len_ += res.size();
  return 0;
}
