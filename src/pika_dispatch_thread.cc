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
                                       int cron_interval, int queue_limit) {
  work_num_ = work_num;
  conn_factory_ = new ClientConnFactory();
  handles_ = new PikaDispatchHandles(this);
  pika_worker_threads_ = new PikaWorkerThread*[work_num_];
  for (int i = 0; i < work_num_; i++) {
    pika_worker_threads_[i] = new PikaWorkerThread(conn_factory_, 1000);
  }
  thread_rep_ = pink::NewDispatchThread(port, work_num_,
                                        reinterpret_cast<pink::Thread**>(pika_worker_threads_),
                                        cron_interval, queue_limit, handles_);
}

PikaDispatchThread::PikaDispatchThread(std::string &ip, int port, int work_num,
                                       int cron_interval, int queue_limit) {
  work_num_ = work_num;
  conn_factory_ = new ClientConnFactory();
  handles_ = new PikaDispatchHandles(this);
  pika_worker_threads_ = new PikaWorkerThread*[work_num_];
  for (int i = 0; i < work_num_; i++) {
    pika_worker_threads_[i] = new PikaWorkerThread(conn_factory_, 1000);
  }
  thread_rep_ = pink::NewDispatchThread(ip, port, work_num_,
                                        reinterpret_cast<pink::Thread**>(pika_worker_threads_),
                                        cron_interval, queue_limit, handles_);
}

PikaDispatchThread::PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                                       int cron_interval, int queue_limit) {
  work_num_ = work_num;
  conn_factory_ = new ClientConnFactory();
  handles_ = new PikaDispatchHandles(this);
  pika_worker_threads_ = new PikaWorkerThread*[work_num_];
  for (int i = 0; i < work_num_; i++) {
    pika_worker_threads_[i] = new PikaWorkerThread(conn_factory_, 1000);
  }
  thread_rep_ = pink::NewDispatchThread(ips, port, work_num_,
                                        reinterpret_cast<pink::Thread**>(pika_worker_threads_),
                                        cron_interval, queue_limit, handles_);
}

PikaDispatchThread::~PikaDispatchThread() {
  thread_rep_->StopThread();
  delete conn_factory_;
  delete handles_;
  for (int i = 0; i < work_num_; i++) {
    delete pika_worker_threads_[i];
  }
  delete[] pika_worker_threads_;
  LOG(INFO) << "dispatch thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
}

int PikaDispatchThread::StartThread() {
  return thread_rep_->StartThread();
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
  for (int i = 0; i < work_num_; i++) {
    num += pika_worker_threads_[i]->ThreadClientNum();
  }
  return num;
}
