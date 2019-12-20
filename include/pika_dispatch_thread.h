// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "include/pika_client_conn.h"

class PikaDispatchThread {
 public:
  PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                     int cron_interval, int queue_limit, int max_conn_rbuf_size);
  ~PikaDispatchThread();
  int StartThread();

  int64_t ThreadClientList(std::vector<ClientInfo> *clients);

  bool ClientKill(const std::string& ip_port);
  void ClientKillAll();

  void SetQueueLimit(int queue_limit) {
    thread_rep_->SetQueueLimit(queue_limit);
  }

 private:
  class ClientConnFactory : public pink::ConnFactory {
   public:
     explicit ClientConnFactory(int max_conn_rbuf_size)
         : max_conn_rbuf_size_(max_conn_rbuf_size) {
     }
     virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::Thread* server_thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const {
       return std::static_pointer_cast<pink::PinkConn>
         (std::make_shared<PikaClientConn>(connfd, ip_port, server_thread, pink_epoll, pink::HandleType::kAsynchronous, max_conn_rbuf_size_));
     }
   private:
     int max_conn_rbuf_size_;
  };

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaDispatchThread* pika_disptcher)
        : pika_disptcher_(pika_disptcher) {
    }
    using pink::ServerHandle::AccessHandle;
    bool AccessHandle(std::string& ip) const override;
    void CronHandle() const override;

   private:
    PikaDispatchThread* pika_disptcher_;
  };

  ClientConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;
};
#endif
