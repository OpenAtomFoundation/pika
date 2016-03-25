#include <glog/logging.h>
#include "pika_binlog_receiver_thread.h"
#include "pika_master_conn.h"
#include "pika_server.h"
#include "pika_command.h"

extern PikaServer* g_pika_server;

PikaBinlogReceiverThread::PikaBinlogReceiverThread(int port, int cron_interval) :
  HolyThread::HolyThread(port, cron_interval),
  thread_querynum_(0),
  last_sec_thread_querynum_(0) {
  cmds_.reserve(300);
  InitCmdTable(&cmds_);
}

PikaBinlogReceiverThread::~PikaBinlogReceiverThread() {
    DestoryCmdTable(cmds_);
    DLOG(INFO) << "BinlogReceiver thread " << thread_id() << " exit!!!";
}

bool PikaBinlogReceiverThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (ThreadClientNum() != 0 || !g_pika_server->ShouldAccessConnAsMaster(ip)) {
    DLOG(INFO) << "BinlogReceiverThread AccessHandle failed";
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
  {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    DLOG(INFO) << "==========Kill Master Sender Conn==============";
    close(iter->first);
    delete(static_cast<PikaMasterConn*>(iter->second));
    iter = conns_.erase(iter);
  }
  }
  g_pika_server->MinusMasterConnection();
}
