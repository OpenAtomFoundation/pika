// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "include/pika_client_conn.h"

class PikaDispatchThread {
 public:
  PikaDispatchThread(std::set<std::string>& ips, int port, int work_num, int cron_interval, int queue_limit,
                     int max_conn_rbuf_size);
  ~PikaDispatchThread();
  int StartThread();

  uint64_t ThreadClientList(std::vector<ClientInfo>* clients);

  bool ClientKill(const std::string& ip_port);
  void ClientKillAll();

  void SetQueueLimit(int queue_limit) { thread_rep_->SetQueueLimit(queue_limit); }

  void UnAuthUserAndKillClient(const std::set<std::string> &users, const std::shared_ptr<User>& defaultUser);

 private:
  class ClientConnFactory : public net::ConnFactory {
   public:
    explicit ClientConnFactory(int max_conn_rbuf_size) : max_conn_rbuf_size_(max_conn_rbuf_size) {}
    std::shared_ptr<net::NetConn> NewNetConn(int connfd, const std::string& ip_port, net::Thread* server_thread,
                                                     void* worker_specific_data, net::NetMultiplexer* net) const override {
      return std::make_shared<PikaClientConn>(connfd, ip_port, server_thread, net, net::HandleType::kAsynchronous, max_conn_rbuf_size_);
    }

   private:
    int max_conn_rbuf_size_ = 0;
  };

  class Handles : public net::ServerHandle {
   public:
    explicit Handles(PikaDispatchThread* pika_disptcher) : pika_disptcher_(pika_disptcher) {}
    using net::ServerHandle::AccessHandle;
    bool AccessHandle(std::string& ip) const override;
    void CronHandle() const override;

   private:
    PikaDispatchThread* pika_disptcher_ = nullptr;
  };

  ClientConnFactory conn_factory_;
  Handles handles_;
  net::ServerThread* thread_rep_ = nullptr;
};
#endif
