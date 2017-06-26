// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "pink/include/pink_conn.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_command.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverThread::PikaBinlogReceiverThread(const std::set<std::string> &ips, int port,
                                                   int cron_interval)
      : conn_factory_(this),
        handles_(this),
        thread_querynum_(0),
        last_thread_querynum_(0),
        last_time_us_(slash::NowMicros()),
        last_sec_thread_querynum_(0),
        serial_(0) {
  thread_rep_ = pink::NewHolyThread(ips, port, &conn_factory_,
                                    cron_interval, &handles_);
  thread_rep_->set_thread_name("BinlogReceiver");
}

PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "BinlogReceiver thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaBinlogReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool PikaBinlogReceiverThread::PikaBinlogReceiverHandles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (binlog_receiver_->ThreadClientNum() != 0 ||
      !g_pika_server->ShouldAccessConnAsMaster(ip)) {
    LOG(WARNING) << "BinlogReceiverThread AccessHandle failed: " << ip;
    return false;
  }
  g_pika_server->PlusMasterConnection();
  return true;
}

void PikaBinlogReceiverThread::KillBinlogSender() {
  thread_rep_->KillAllConns();
}
