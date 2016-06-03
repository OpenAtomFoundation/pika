#include <glog/logging.h>
#include "binlog_receiver_thread.h"
#include "master_conn.h"
#include "binlog_sync.h"

extern BinlogSync* g_binlog_sync;

BinlogReceiverThread::BinlogReceiverThread(int port, int cron_interval) :
  HolyThread::HolyThread(port, cron_interval) {
}

BinlogReceiverThread::~BinlogReceiverThread() {
  DLOG(INFO) << "BinlogReceiver thread " << thread_id() << " exit!!!";
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
  {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    DLOG(INFO) << "==========Kill Master Sender Conn==============";
    close(iter->first);
    delete(static_cast<MasterConn*>(iter->second));
    iter = conns_.erase(iter);
  }
  }
  g_binlog_sync->MinusMasterConnection();
}
