// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include "pink/include/server_thread.h"
#include "pika_worker_thread.h"
#include "pika_client_conn.h"

class PikaDispatchThread {
 public:
  PikaDispatchThread(int port, int work_num,
                     int cron_interval, int queue_limit);
  PikaDispatchThread(std::string &ip, int port, int work_num,
                     int cron_interval, int queue_limit);
  PikaDispatchThread(std::set<std::string> &ips, int port, int work_num,
                     int cron_interval, int queue_limit);
  ~PikaDispatchThread();
  int StartThread();

  PikaWorkerThread** pika_worker_threads() {
    return pika_worker_threads_;
  }
  int ClientNum();

 private:
  class ClientConnFactory : public pink::ConnFactory {
   public:
    virtual pink::PinkConn *NewPinkConn(int connfd,
                                        const std::string &ip_port,
                                        pink::Thread *thread) const {
      return new PikaClientConn(connfd, ip_port, thread);
    }
  };

  class PikaDispatchHandles : public pink::ServerHandle {
   public:
    explicit PikaDispatchHandles(PikaDispatchThread* pika_disptcher)
        : pika_disptcher_(pika_disptcher) {
    }
    void AccessHandle(std::string& ip) {
      pika_disptcher_->AccessHandle(ip);
    }

   private:
    PikaDispatchThread* pika_disptcher_;
  };


  ClientConnFactory* conn_factory_;
  PikaDispatchHandles* handles_;
  int work_num_;
  PikaWorkerThread** pika_worker_threads_;
  pink::ServerThread* thread_rep_;

  bool AccessHandle(std::string& ip);
};
#endif
