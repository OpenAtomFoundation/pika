// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PROXY_CLI_H_
#define PIKA_PROXY_CLI_H_

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"

#include "include/pika_proxy_conn.h"

using slash::Status;
class ProxyCli;
class ProxyFactory : public pink::ConnFactory {
 public:
  explicit ProxyFactory(std::shared_ptr<ProxyCli> proxy_cli);
  virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
      int connfd,
      const std::string &ip_port,
      pink::Thread *thread,
      void* worker_specific_data,
      pink::PinkEpoll* pink_epoll) const override {
    return std::static_pointer_cast<pink::PinkConn>
      (std::make_shared<PikaProxyConn>(
         connfd, ip_port, thread, pink_epoll, proxy_cli_));
  }
 private:
  std::shared_ptr<ProxyCli> proxy_cli_;
};

class ProxyHandle : public pink::ClientHandle {
 public:
  explicit ProxyHandle(std::shared_ptr<ProxyCli> proxy_cli) : ClientHandle() {
    proxy_cli_ = proxy_cli;
  }
  ~ProxyHandle() {
  }
  void CronHandle() const override {
  }
  void FdTimeoutHandle(int fd, const std::string& ip_port) const override {
  }
  void FdClosedHandle(int fd, const std::string& ip_port) const override;
  bool AccessHandle(std::string& ip) const override {
    return true;
  }
  int CreateWorkerSpecificData(void** data) const override {
    return 0;
  }
  int DeleteWorkerSpecificData(void* data) const override {
    return 0;
  }
  void DestConnectFailedHandle(
      std::string ip_port, std::string reason) const override {
  }

 private:
  std::shared_ptr<ProxyCli> proxy_cli_;
};

class ProxyCli : public std::enable_shared_from_this<ProxyCli> {
 public:
  ProxyCli(int cron_interval, int keepalive_timeout);
  int Start();
  int Stop();
  Status ForwardToBackend(ProxyTask* task);
  Status WritebackUpdate(const std::string& ip_port,
      const std::string& res, bool* write_back, ProxyTask** res_task);

  struct ProxyCliTask {
    std::shared_ptr<PikaClientConn> conn_ptr;
    std::shared_ptr<std::string> resp_ptr;
  };
  void LostConn(const std::string& ip_port);

 private:
  int cron_interval_;
  int keepalive_timeout_;
  ProxyFactory* proxy_factory_;
  ProxyHandle* proxy_handle_;

  slash::Mutex input_l_;
  std::shared_ptr<pink::ClientThread> client_ptr_;
  // pair(backend conn ip + port, std::deque<ProxyCliTask>)
  std::unordered_map<std::string, std::deque<ProxyCliTask>> backend_task_queue_;
  // pair(client ip + port, ProxyTask*)
  std::unordered_map<std::string, ProxyTask*> task_queue_;
};

#endif  // PIKA_PROXY_CLI_H_
#
