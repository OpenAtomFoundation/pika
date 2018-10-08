// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_HEARTBEAT_THREAD_H_
#define PIKA_HEARTBEAT_THREAD_H_

#include <set>
#include <string>

#include "pink/include/server_thread.h"
#include "pika_heartbeat_conn.h"

class PikaHeartbeatThread {
 public:
  PikaHeartbeatThread(std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaHeartbeatThread();

  int StartThread();

 private:
  class HeartbeatConnFactory : public pink::ConnFactory {
   public:
    virtual std::shared_ptr<pink::PinkConn> NewPinkConn(
        int connfd,
        const std::string &ip_port,
        pink::ServerThread *thread,
        void* worker_specific_data,
        pink::PinkEpoll* pink_epoll) const override {
      return std::make_shared<PikaHeartbeatConn>(connfd, ip_port);
    }
  };

  class Handles : public pink::ServerHandle {
   public:
    explicit Handles(PikaHeartbeatThread* heartbeat_thread)
        : heartbeat_thread_(heartbeat_thread) {
    }
    void CronHandle() const override;
    void FdClosedHandle(int fd, const std::string& ip_port) const override;
    void FdTimeoutHandle(int fd, const std::string& ip_port) const override;
    using pink::ServerHandle::AccessHandle;
    bool AccessHandle(std::string& ip) const override;

   private:
    PikaHeartbeatThread* heartbeat_thread_;
  };

 private:
  HeartbeatConnFactory conn_factory_;
  Handles handles_;
  pink::ServerThread* thread_rep_;
};

#endif
