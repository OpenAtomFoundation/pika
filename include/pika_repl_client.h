// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_REPL_CLIENT_H_
#define PIKA_REPL_CLIENT_H_

#include <string>
#include <memory>

#include "include/pika_repl_client_conn.h"
#include "pink/include/pink_conn.h"
#include "pink/include/client_thread.h"
#include "pink/include/thread_pool.h"

class PikaReplClient {
 public:
  PikaReplClient(int cron_interval, int keepalive_timeout);
  ~PikaReplClient();
  void Write(const std::string& ip, const int port, const std::string& msg);
  void ThreadPollSchedule(pink::TaskFunc func, void*arg);
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
      return std::make_shared<PikaReplClientConn>(connfd, ip_port, thread, worker_specific_data);
    }
  };
  ReplClientConnFactory conn_factory_;
  int cron_interval_;
  int keepalive_timeout_;
  pink::ClientThread* client_thread_;
};

#endif
