// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include <queue>
#include <set>

#include "pink/include/server_thread.h"
#include "slash/include/slash_mutex.h"
#include "slash/include/env.h"
#include "pika_define.h"
#include "pika_master_conn.h"
#include "pika_command.h"

class PikaBinlogReceiverThread {
 public:
  PikaBinlogReceiverThread(std::set<std::string> &ips, int port, int cron_interval = 0);
  ~PikaBinlogReceiverThread();
  void KillBinlogSender();
  int StartThread();

  uint64_t thread_querynum() {
    slash::RWLock l(&rwlock_, false);
    return thread_querynum_;
  }

  void ResetThreadQuerynum() {
    slash::RWLock l(&rwlock_, true);
    thread_querynum_ = 0;
    last_thread_querynum_ = 0;
  }

  uint64_t last_sec_thread_querynum() {
    slash::RWLock l(&rwlock_, false);
    return last_sec_thread_querynum_;
  }

  uint64_t GetnPlusSerial() {
    return serial_++;
  }

  void PlusThreadQuerynum() {
    slash::RWLock l(&rwlock_, true);
    thread_querynum_++;
  }

  void ResetLastSecQuerynum() {
    uint64_t cur_time_ms = slash::NowMicros();
    slash::RWLock l(&rwlock_, true);
    last_sec_thread_querynum_ = (thread_querynum_ - last_thread_querynum_) * 1000000 / (cur_time_ms - last_time_us_+1);
    last_time_us_ = cur_time_ms;
    last_thread_querynum_ = thread_querynum_;
  }

  int32_t ThreadClientNum() {
    return thread_rep_->conn_num();
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }

 private:
  class MasterConnFactory : public pink::ConnFactory {
   public:
    MasterConnFactory(PikaBinlogReceiverThread* binlog_receiver)
        : binlog_receiver_(binlog_receiver) {
    }
    virtual pink::PinkConn *NewPinkConn(int connfd,
                                        const std::string &ip_port,
                                        pink::Thread *thread) const {
      return new PikaMasterConn(connfd, ip_port, binlog_receiver_);
    }

   private:
    PikaBinlogReceiverThread* binlog_receiver_;
  };

  class PikaBinlogReceiverHandles : public pink::ServerHandle {
   public:
    explicit PikaBinlogReceiverHandles(PikaBinlogReceiverThread* binlog_receiver)
        : binlog_receiver_(binlog_receiver) {
    }
    void CronHandle() const {
      binlog_receiver_->CronHandle();
    }
    bool AccessHandle(std::string& ip) const {
      return binlog_receiver_->AccessHandle(ip);
    }

   private:
    PikaBinlogReceiverThread* binlog_receiver_;
  };

  MasterConnFactory* conn_factory_;
  PikaBinlogReceiverHandles* handles_;
  pink::ServerThread* thread_rep_;

  bool AccessHandle(std::string& ip);
  void CronHandle();
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  slash::Mutex mutex_; // protect cron_task_
  std::queue<WorkerCronTask> cron_tasks_;

  pthread_rwlock_t rwlock_;
  std::unordered_map<std::string, Cmd*> cmds_;
  uint64_t thread_querynum_;
  uint64_t last_thread_querynum_;
  uint64_t last_time_us_;
  uint64_t last_sec_thread_querynum_;
  uint64_t serial_;
};
#endif
