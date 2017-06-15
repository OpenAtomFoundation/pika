// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_RECEIVER_THREAD_H_
#define BINLOG_RECEIVER_THREAD_H_

#include <queue>

#include "pink/include/server_thread.h"
#include "slash/include/slash_mutex.h"
#include "pika_define.h"
#include "master_conn.h"

class BinlogReceiverThread {
 public:
  BinlogReceiverThread(int port, int cron_interval = 0);
  virtual ~BinlogReceiverThread();
	int StartThread();

  void KillBinlogSender();
  int32_t ThreadClientNum() {
    return thread_rep_->conn_num();
  }

 private:
  class MasterConnFactory : public pink::ConnFactory {
   public:
    virtual pink::PinkConn *NewPinkConn(int connfd,
                                        const std::string &ip_port,
                                        pink::Thread *thread) const {
      return new MasterConn(connfd, ip_port, thread);
    }
  };

  class PikaBinlogReceiverHandles : public pink::ServerHandle {
   public:
    explicit PikaBinlogReceiverHandles(BinlogReceiverThread* binlog_receiver)
        : binlog_receiver_(binlog_receiver) {
    }
    void CronHandle() {
      binlog_receiver_->CronHandle();
    }
    void AccessHandle(std::string& ip) {
      binlog_receiver_->AccessHandle(ip);
    }

   private:
    BinlogReceiverThread* binlog_receiver_;
  };

  MasterConnFactory* conn_factory_;
  PikaBinlogReceiverHandles* handles_;
  pink::ServerThread* thread_rep_;

  slash::Mutex mutex_; // protect cron_task_
  std::queue<WorkerCronTask> cron_tasks_;

  void AddCronTask(WorkerCronTask task);
  void KillAll();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
};
#endif
