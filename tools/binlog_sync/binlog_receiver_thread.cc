// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>

#include "pink/include/pink_conn.h"
#include "binlog_receiver_thread.h"
#include "master_conn.h"
#include "binlog_sync.h"

extern BinlogSync* g_binlog_sync;

BinlogReceiverThread::BinlogReceiverThread(int port, int cron_interval) {
  conn_factory_ = new MasterConnFactory(this);
  handles_ = new PikaBinlogReceiverHandles(this);
  thread_rep_ = pink::NewHolyThread(port, conn_factory_,
                                    cron_interval, handles_);
}

BinlogReceiverThread::~BinlogReceiverThread() {
  thread_rep_->StopThread();
  delete conn_factory_;
  delete handles_;;
  DLOG(INFO) << "BinlogReceiver thread " << thread_rep_->thread_id() << " exit!!!";
	delete thread_rep_;
}

int BinlogReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool BinlogReceiverThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_binlog_sync->host();
  }
  if (ThreadClientNum() != 0 || !g_binlog_sync->ShouldAccessConnAsMaster(ip)) {
    DLOG(INFO) << "BinlogReceiverThread AccessHandle failed";
    return false;
  }
  g_binlog_sync->PlusMasterConnection();
  return true;
}

void BinlogReceiverThread::CronHandle() {
  {
  WorkerCronTask t;
  slash::MutexLock l(&mutex_);

  while(!cron_tasks_.empty()) {
    t = cron_tasks_.front();
    cron_tasks_.pop();
    mutex_.Unlock();
    DLOG(INFO) << "BinlogReceiverThread, Got a WorkerCronTask";
    switch (t.task) {
      case TASK_KILL:
        break;
      case TASK_KILLALL:
        KillAll();
        break;
    }
    mutex_.Lock();
  }
  }
}

void BinlogReceiverThread::KillBinlogSender() {
  AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
}

void BinlogReceiverThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

void BinlogReceiverThread::KillAll() {
  thread_rep_->Cleanup();
  g_binlog_sync->MinusMasterConnection();
}
