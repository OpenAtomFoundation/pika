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
  PikaHeartbeatThread(std::string &ip, int port, int cron_interval = 0);
  PikaHeartbeatThread(std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaHeartbeatThread();

  int StartThread();

  bool FindSlave(int fd); //hb_fd

 private:
  class HeartbeatConnFactory : public pink::ConnFactory {
   public:
    virtual pink::PinkConn *NewPinkConn(int connfd,
                                        const std::string &ip_port,
                                        pink::Thread *thread) const override {
      return new PikaHeartbeatConn(connfd, ip_port);
    }
  };

  class PikaHeartbeatHandles : public pink::ServerHandle {
   public:
    explicit PikaHeartbeatHandles(PikaHeartbeatThread* heartbeat_thread)
        : heartbeat_thread_(heartbeat_thread) {
    }
    void CronHandle() const override {
      heartbeat_thread_->CronHandle();
    }
    bool AccessHandle(std::string& ip) const override {
      return heartbeat_thread_->AccessHandle(ip);
    }

   private:
    PikaHeartbeatThread* heartbeat_thread_;
  };

 private:
  HeartbeatConnFactory* conn_factory_;
  PikaHeartbeatHandles* handles_;
  pink::ServerThread* thread_rep_;

  void CronHandle();
  bool AccessHandle(std::string& ip_port);
};

#endif
