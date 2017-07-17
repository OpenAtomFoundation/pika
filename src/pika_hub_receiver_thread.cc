// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "pink/include/pink_conn.h"
#include "pika_hub_receiver_thread.h"
#include "pika_server.h"
#include "pika_command.h"

extern PikaServer* g_pika_server;

PikaHubReceiverThread::PikaHubReceiverThread(const std::set<std::string> &ips, int port,
                                             int cron_interval)
      : conn_factory_(this),
        handles_(this),
        serial_(0) {
  thread_rep_ = pink::NewHolyThread(ips, port, &conn_factory_,
                                    cron_interval, &handles_);
  thread_rep_->set_thread_name("HubReceiver");
  thread_rep_->set_keepalive_timeout(0);
}

PikaHubReceiverThread::~PikaHubReceiverThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "BinlogReceiver thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaHubReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool PikaHubReceiverThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (hub_receiver_->thread_rep_->conn_num() != 0 ||
      !g_pika_server->IsHub(ip)) {
    LOG(WARNING) << "HubReceiverThread AccessHandle failed: " << ip;
    return false;
  }
  g_pika_server->HubConnected();
  return true;
}

void PikaHubReceiverThread::Handles::FdClosedHandle(
        int fd, const std::string& ip_port) const {
  LOG(INFO) << "HubReceiverThread Fd closed: " << ip_port;
  g_pika_server->DeleteHub();
}
