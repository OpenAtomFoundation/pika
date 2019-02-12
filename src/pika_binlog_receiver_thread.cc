// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_binlog_receiver_thread.h"

#include "include/pika_server.h"
#include "include/pika_repl_server_thread.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverThread::PikaBinlogReceiverThread(const std::set<std::string> &ips, int port,
                                                   int cron_interval)
      : conn_factory_(this),
        handles_(this),
        serial_(0) {
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
  thread_rep_ = new PikaReplServerThread(ips, port, cron_interval);
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

bool PikaBinlogReceiverThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  LOG(INFO) << "Master Binlog Sender: " << ip << " connecting";
  if (!g_pika_server->ShouldAccessConnAsMaster(ip)) {
   LOG(WARNING) << "BinlogReceiverThread AccessHandle failed: " << ip;
   return false;
  }
  g_pika_server->PlusMasterConnection();
  return true;
}

void PikaBinlogReceiverThread::KillBinlogSender() {
  thread_rep_->KillAllConns();
  // FIXME (gaodq) do in crontask ?
  g_pika_server->MinusMasterConnection();
}
