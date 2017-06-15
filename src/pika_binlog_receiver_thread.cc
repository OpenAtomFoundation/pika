// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <glog/logging.h>
#include "pink/include/pink_conn.h"
#include "pika_binlog_receiver_thread.h"
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_command.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverThread::PikaBinlogReceiverThread(std::string &ip, int port,
                                                   int cron_interval)
    : thread_querynum_(0),
      last_thread_querynum_(0),
      last_time_us_(slash::NowMicros()),
      last_sec_thread_querynum_(0),
      serial_(0) {
  conn_factory_ = new MasterConnFactory();
  handles_ = new PikaBinlogReceiverHandles(this);
  thread_rep_ = pink::NewHolyThread(ip, port, conn_factory_,
                                    cron_interval, handles_);
  pthread_rwlock_init(&rwlock_, nullptr);
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
}

PikaBinlogReceiverThread::PikaBinlogReceiverThread(std::set<std::string> &ips, int port,
                                                   int cron_interval)
    : thread_querynum_(0),
      last_thread_querynum_(0),
      last_time_us_(slash::NowMicros()),
      last_sec_thread_querynum_(0),
      serial_(0) {
  conn_factory_ = new MasterConnFactory();
  handles_ = new PikaBinlogReceiverHandles(this);
  thread_rep_ = pink::NewHolyThread(ips, port, conn_factory_,
                                    cron_interval, handles_);
  pthread_rwlock_init(&rwlock_, nullptr);
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
}

PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {
  thread_rep_->StopThread();
  delete handles_;
  delete conn_factory_;
  DestoryCmdTable(cmds_);
  LOG(INFO) << "BinlogReceiver thread " << thread_rep_->thread_id() << " exit!!!";
  delete thread_rep_;
  pthread_rwlock_destroy(&rwlock_);
}

int PikaBinlogReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool PikaBinlogReceiverThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (ThreadClientNum() != 0 || !g_pika_server->ShouldAccessConnAsMaster(ip)) {
    LOG(WARNING) << "BinlogReceiverThread AccessHandle failed: " << ip;
    return false;
  }
  g_pika_server->PlusMasterConnection();
  return true;
}

void PikaBinlogReceiverThread::CronHandle() {
  ResetLastSecQuerynum();
  {
  WorkerCronTask t;
  slash::MutexLock l(&mutex_);

  while(!cron_tasks_.empty()) {
    t = cron_tasks_.front();
    cron_tasks_.pop();
    mutex_.Unlock();
    DLOG(INFO) << "PikaBinlogReceiverThread, Got a WorkerCronTask";
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

void PikaBinlogReceiverThread::KillBinlogSender() {
  AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
}

void PikaBinlogReceiverThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

void PikaBinlogReceiverThread::KillAll() {
  thread_rep_->Cleanup();
  g_pika_server->MinusMasterConnection();
}
