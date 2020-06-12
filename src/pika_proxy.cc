// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_proxy.h"
#include "include/pika_server.h"

extern PikaProxy* g_pika_proxy;
extern PikaServer* g_pika_server;

/* ProxyCliManager */

ProxyCliManager::ProxyCliManager(
    int conn_every_backend, int keepalive_time)
  : rr_counter_(0), conn_every_backend_(conn_every_backend) {
  for (int i = 0; i < conn_every_backend; ++i) {
    clis_.push_back(
        std::make_shared<ProxyCli>(10 /*cron interval*/, keepalive_time));
  }
}

ProxyCliManager::~ProxyCliManager() {
}

Status ProxyCliManager::ChooseForwardToBackend(ProxyTask* task) {
  return ForwardNextAvailableConn(task);
}

Status ProxyCliManager::ForwardNextAvailableConn(ProxyTask* task) {
  uint64_t counter = rr_counter_.load() % conn_every_backend_;
  rr_counter_++;
  return clis_[counter]->ForwardToBackend(task);
}

int ProxyCliManager::Start() {
  for (auto& cli : clis_) {
    int res = cli->Start();
    if (res != pink::kSuccess) {
      LOG(ERROR) << "ProxyCliManager Start Failed: " <<
        (res == pink::kCreateThreadError ?
         ": create thread error " : ": other error");
      return res;
    }
  }
  return pink::kSuccess;
}

int ProxyCliManager::Stop() {
  for (auto& cli : clis_) {
    cli->Stop();
  }
  return pink::kSuccess;
}

/* PikaProxy */

PikaProxy::PikaProxy() {
  // conn_every_backend: 10, keepalive_time: 60s
  cli_manager_ptr_ = std::make_shared<ProxyCliManager>(10, 60);
}

PikaProxy::~PikaProxy() {
}

int PikaProxy::Start() {
  int res = cli_manager_ptr_->Start();
  if (res != pink::kSuccess) {
    LOG(ERROR) << "PikaProxy Start Failed: " <<
      (res == pink::kCreateThreadError ?
       ": create thread error " : ": other error");
    return res;
  }
  return pink::kSuccess;
}

int PikaProxy::Stop() {
  cli_manager_ptr_->Stop();
  return pink::kSuccess;
}

void PikaProxy::ForwardToBackend(void* arg) {
  ProxyTask* task = reinterpret_cast<ProxyTask*>(arg);
  Status s = g_pika_proxy->cli_manager()->ChooseForwardToBackend(task);
  if (!s.ok()) {
    delete task;
    task = NULL;
    LOG(WARNING) << "Forward to backend" << s.ToString();
  }
}

// just one therad invoke this, no lock guard
void PikaProxy::WritebackToCliConn(void* arg) {
  ProxyTask* task = reinterpret_cast<ProxyTask*>(arg);
  std::shared_ptr<PikaClientConn> conn_ptr = task->conn_ptr;
  // TODO(AZ) build redis resp
  // for (auto& resp : conn_ptr->resp_array) {
  //   conn_ptr->WriteResp(std::move(*resp) + "\r\n");
  // }
  conn_ptr->resp_array.clear();
  conn_ptr->NotifyEpoll(true);
  delete task;
}

void PikaProxy::MayScheduleWritebackToCliConn(
    std::shared_ptr<PikaProxyConn> conn_ptr,
    std::shared_ptr<ProxyCli> cli, const std::string res) {
  bool write_back = false;
  ProxyTask* task = NULL;
  Status s = cli->WritebackUpdate(conn_ptr->ip_port(), res, &write_back, &task);
  if (!s.ok()) {
    LOG(WARNING) << "WritebaclUpdate failed " + s.ToString();
    return;
  }
  if (!write_back) {
    return;
  }
  g_pika_server->ScheduleClientPool(&PikaProxy::WritebackToCliConn, task);
}

void PikaProxy::ScheduleForwardToBackend(
    const std::shared_ptr<PikaClientConn>& conn_ptr,
    const std::vector<pink::RedisCmdArgsType>& redis_cmds,
    const std::vector<Node>& dst) {
  ProxyTask* arg = new ProxyTask();
  arg->conn_ptr = conn_ptr;
  arg->redis_cmds = std::move(redis_cmds);
  arg->redis_cmds_forward_dst = std::move(dst);
  // choose ip and update
  g_pika_server->ScheduleClientPool(&PikaProxy::ForwardToBackend, arg);
}
