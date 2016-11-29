// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_RECEIVER_THREAD_H_
#define BINLOG_RECEIVER_THREAD_H_

#include <queue>

#include "holy_thread.h"
#include "slash_mutex.h"
#include "pika_define.h"
#include "master_conn.h"

class BinlogReceiverThread : public pink::HolyThread<MasterConn>
{
public:
  BinlogReceiverThread(int port, int cron_interval = 0);
  virtual ~BinlogReceiverThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
  void KillBinlogSender();
  int32_t ThreadClientNum() {
    slash::RWLock(&rwlock_, false);
    int32_t num = conns_.size();
    return num;
  }


private:
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

};
#endif
