// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "include/pika_dispatch_thread.h"
#include "include/pika_client_conn.h"
#include "include/pika_server.h"
#include "include/pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaDispatchThread::PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                                       int cron_interval, int queue_limit)
      : handles_(this) {
  thread_rep_ = pink::NewDispatchThread(ips, port, work_num, &conn_factory_,
                                        cron_interval, queue_limit, &handles_);
  thread_rep_->set_thread_name("Dispatcher");
}

PikaDispatchThread::~PikaDispatchThread() {
  thread_rep_->StopThread();
  LOG(INFO) << "dispatch thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaDispatchThread::StartThread() {
  return thread_rep_->StartThread();
}

int64_t PikaDispatchThread::ThreadClientList(std::vector<ClientInfo> *clients) {
  std::vector<pink::ServerThread::ConnInfo> conns_info =
    thread_rep_->conns_info();
  if (clients != nullptr) {
    for (auto& info : conns_info) {
      clients->push_back({
                          info.fd,
                          info.ip_port,
                          info.last_interaction.tv_sec,
                          nullptr /* PinkConn pointer, doesn't need here */
                         });
    }
  }
  return conns_info.size();
}

bool PikaDispatchThread::ClientKill(const std::string& ip_port) {
  return thread_rep_->KillConn(ip_port);
}

void PikaDispatchThread::ClientKillAll() {
  thread_rep_->KillAllConns();
}

bool PikaDispatchThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }

  int client_num = pika_disptcher_->thread_rep_->conn_num();
  if ((client_num >= g_pika_conf->maxclients() + g_pika_conf->root_connection_num())
      || (client_num >= g_pika_conf->maxclients() && ip != g_pika_server->host())) {
    LOG(WARNING) << "Max connections reach, Deny new comming: " << ip;
    return false;
  }

  DLOG(INFO) << "new clinet comming, ip: " << ip;
  g_pika_server->incr_accumulative_connections();
  return true;
}

void PikaDispatchThread::Handles::CronHandle() const {
  pika_disptcher_->thread_rep_->set_keepalive_timeout(g_pika_conf->timeout());
  g_pika_server->ResetLastSecQuerynum();
}
