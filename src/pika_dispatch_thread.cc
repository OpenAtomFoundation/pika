// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "pika_dispatch_thread.h"
#include "pika_client_conn.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaDispatchThread::PikaDispatchThread(int port, int work_num,
                                       PikaWorkerThread** pika_worker_thread,
                                       int cron_interval, int queue_limit) :
  DispatchThread::DispatchThread(port, work_num,
                                 reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread),
                                 cron_interval, queue_limit) {
}

PikaDispatchThread::PikaDispatchThread(std::string &ip, int port, int work_num,
                                       PikaWorkerThread** pika_worker_thread,
                                       int cron_interval, int queue_limit) :
  DispatchThread::DispatchThread(ip, port, work_num,
                                 reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread),
                                 cron_interval, queue_limit) {
}

PikaDispatchThread::PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                                       PikaWorkerThread** pika_worker_thread,
                                       int cron_interval, int queue_limit) :
  DispatchThread::DispatchThread(ips, port, work_num,
                                 reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread),
                                 cron_interval, queue_limit) {
}

PikaDispatchThread::~PikaDispatchThread() {
  LOG(INFO) << "dispatch thread " << thread_id() << " exit!!!";
}

bool PikaDispatchThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }

  int client_num = ClientNum();
  if ((client_num >= g_pika_conf->maxclients() + g_pika_conf->root_connection_num())
      || (client_num >= g_pika_conf->maxclients() && ip != g_pika_server->host())) {
    LOG(WARNING) << "Max connections reach, Deny new comming: " << ip;
    return false;
  }

  DLOG(INFO) << "new clinet comming, ip: " << ip;
  g_pika_server->incr_accumulative_connections();
  return true;
}

int PikaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((PikaWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
