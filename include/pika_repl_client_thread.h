// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_THREAD_H_
#define PIKA_REPL_CLIENT_THREAD_H_

#include <string>
#include <memory>

#include "include/pika_repl_client_conn.h"

#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"

class PikaReplClientThread : public pink::ClientThread {
 public:
  PikaReplClientThread(int cron_interval, int keepalive_timeout);
  virtual ~PikaReplClientThread() = default;
  int Start();

 private:
  class ReplClientConnFactory : public pink::ConnFactory {
   public:
    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::Thread *thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const override {
      return std::make_shared<PikaReplClientConn>(connfd, ip_port, thread, worker_specific_data, pink_epoll);
    }
  };
  class ReplClientHandle : public pink::ClientHandle {
   public:
    void CronHandle() const override {
    }
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    bool AccessHandle(std::string& ip) const override {
      // ban 127.0.0.1 if you want to test this routine
      // if (ip.find("127.0.0.2") != std::string::npos) {
      //   std::cout << "AccessHandle " << ip << std::endl;
      //   return false;
      // }
      return true;
    }
    int CreateWorkerSpecificData(void** data) const override {
      return 0;
    }
    int DeleteWorkerSpecificData(void* data) const override {
      return 0;
    }
    void DestConnectFailedHandle(std::string ip_port, std::string reason) const override {
    }
  };

  ReplClientConnFactory conn_factory_;
  ReplClientHandle handle_;
};

#endif  // PIKA_REPL_CLIENT_THREAD_H_
