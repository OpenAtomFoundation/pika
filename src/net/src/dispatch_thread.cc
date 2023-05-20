// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <vector>

#include <glog/logging.h>

#include "net/src/dispatch_thread.h"
#include "net/src/net_item.h"
#include "net/src/net_multiplexer.h"
#include "net/src/worker_thread.h"

namespace net {

DispatchThread::DispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval, int queue_limit,
                               const ServerHandle* handle)
    : ServerThread::ServerThread(port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));
  }
}

DispatchThread::DispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory,
                               int cron_interval, int queue_limit, const ServerHandle* handle)
    : ServerThread::ServerThread(ip, port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {

  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));
  }
}

DispatchThread::DispatchThread(const std::set<std::string>& ips, int port, int work_num, ConnFactory* conn_factory,
                               int cron_interval, int queue_limit, const ServerHandle* handle)
    : ServerThread::ServerThread(ips, port, cron_interval, handle),
      last_thread_(0),
      work_num_(work_num),
      queue_limit_(queue_limit) {

  for (int i = 0; i < work_num_; i++) {
    worker_thread_.emplace_back(std::make_unique<WorkerThread>(conn_factory, this, queue_limit, cron_interval));

  }
}

DispatchThread::~DispatchThread() {

}

int DispatchThread::StartThread() {
  for (int i = 0; i < work_num_; i++) {
    int ret = handle_->CreateWorkerSpecificData(&(worker_thread_[i]->private_data_));
    if (ret != 0) {
      return ret;
    }

    if (!thread_name().empty()) {
      worker_thread_[i]->set_thread_name("WorkerThread");
    }
    ret = worker_thread_[i]->StartThread();
    if (ret != 0) {
      return ret;
    }
  }
  return ServerThread::StartThread();
}

int DispatchThread::StopThread() {
  for (int i = 0; i < work_num_; i++) {
    worker_thread_[i]->set_should_stop();
  }
  for (int i = 0; i < work_num_; i++) {
    int ret = worker_thread_[i]->StopThread();
    if (ret != 0) {
      return ret;
    }
    if (worker_thread_[i]->private_data_ != nullptr) {
      ret = handle_->DeleteWorkerSpecificData(worker_thread_[i]->private_data_);
      if (ret != 0) {
        return ret;
      }
      worker_thread_[i]->private_data_ = nullptr;
    }
  }
  return ServerThread::StopThread();
}

void DispatchThread::set_keepalive_timeout(int timeout) {
  for (int i = 0; i < work_num_; ++i) {
    worker_thread_[i]->set_keepalive_timeout(timeout);
  }
}

int DispatchThread::conn_num() const {
  int conn_num = 0;
  for (int i = 0; i < work_num_; ++i) {
    conn_num += worker_thread_[i]->conn_num();
  }
  return conn_num;
}

std::vector<ServerThread::ConnInfo> DispatchThread::conns_info() const {
  std::vector<ServerThread::ConnInfo> result;
  for (int i = 0; i < work_num_; ++i) {
    const auto worker_conns_info = worker_thread_[i]->conns_info();
    result.insert(result.end(), worker_conns_info.begin(), worker_conns_info.end());
  }
  return result;
}

std::shared_ptr<NetConn> DispatchThread::MoveConnOut(int fd) {
  for (int i = 0; i < work_num_; ++i) {
    std::shared_ptr<NetConn> conn = worker_thread_[i]->MoveConnOut(fd);
    if (conn != nullptr) {
      return conn;
    }
  }
  return nullptr;
}

void DispatchThread::MoveConnIn(std::shared_ptr<NetConn> conn, const NotifyType& type) {
  std::unique_ptr<WorkerThread>& worker_thread = worker_thread_[last_thread_];
  bool success = worker_thread->MoveConnIn(conn, type, true);
  if (success) {
    last_thread_ = (last_thread_ + 1) % work_num_;
    conn->set_net_multiplexer(worker_thread->net_multiplexer());
  }
}

bool DispatchThread::KillConn(const std::string& ip_port) {
  bool result = false;
  for (int i = 0; i < work_num_; ++i) {
    result = worker_thread_[i]->TryKillConn(ip_port) || result;
  }
  return result;
}

void DispatchThread::KillAllConns() { KillConn(kKillAllConnsTask); }

void DispatchThread::HandleNewConn(const int connfd, const std::string& ip_port) {
  // Slow workers may consume many fds.
  // We simply loop to find next legal worker.
  NetItem ti(connfd, ip_port);
  LOG(INFO) << "accept new conn " << ti.String();
  int next_thread = last_thread_;
  bool find = false;
  for (int cnt = 0; cnt < work_num_; cnt++) {
    std::unique_ptr<WorkerThread>& worker_thread = worker_thread_[next_thread];
    find = worker_thread->MoveConnIn(ti, false);
    if (find) {
      last_thread_ = (next_thread + 1) % work_num_;
      LOG(INFO) << "find worker(" << next_thread << "), refresh the last_thread_ to " << last_thread_;
      break;
    }
    next_thread = (next_thread + 1) % work_num_;
  }

  if (!find) {
    LOG(INFO) << "all workers are full, queue limit is " << queue_limit_;
    // every worker is full
    // TODO(anan) maybe add log
    close(connfd);
  }
}

void DispatchThread::SetQueueLimit(int queue_limit) { queue_limit_ = queue_limit; }

extern ServerThread* NewDispatchThread(int port, int work_num, ConnFactory* conn_factory, int cron_interval,
                                       int queue_limit, const ServerHandle* handle) {
  return new DispatchThread(port, work_num, conn_factory, cron_interval, queue_limit, handle);
}
extern ServerThread* NewDispatchThread(const std::string& ip, int port, int work_num, ConnFactory* conn_factory,
                                       int cron_interval, int queue_limit, const ServerHandle* handle) {
  return new DispatchThread(ip, port, work_num, conn_factory, cron_interval, queue_limit, handle);
}
extern ServerThread* NewDispatchThread(const std::set<std::string>& ips, int port, int work_num,
                                       ConnFactory* conn_factory, int cron_interval, int queue_limit,
                                       const ServerHandle* handle) {
  return new DispatchThread(ips, port, work_num, conn_factory, cron_interval, queue_limit, handle);
}

};  // namespace net
