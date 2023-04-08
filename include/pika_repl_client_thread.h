// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_THREAD_H_
#define PIKA_REPL_CLIENT_THREAD_H_

#include <string>
#include <memory>

#include "include/pika_repl_client_conn.h"

#include "net/include/net_conn.h"
#include "net/include/client_thread.h"

class PikaReplClientThread : public net::ClientThread {
 public:
  PikaReplClientThread(int cron_interval, int keepalive_timeout);
  virtual ~PikaReplClientThread() = default;
  int Start();

 private:
  class ReplClientConnFactory : public net::ConnFactory {
   public:
    virtual std::shared_ptr<net::NetConn> NewNetConn(
        int connfd,
        const std::string &ip_port,
        net::Thread *thread,
        void* worker_specific_data,
        net::NetMultiplexer* net) const override {
      return std::static_pointer_cast<net::NetConn>
        (std::make_shared<PikaReplClientConn>(connfd, ip_port, thread, worker_specific_data, net));
    }
  };
  class ReplClientHandle : public net::ClientHandle {
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
