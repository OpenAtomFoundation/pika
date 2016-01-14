#include "pika_worker_thread.h"

namespace pika {
PikaWorkerThread::PikaWorkerThread(int cron_interval):
  WorkerThread::WorkerThread(cron_interval) {
}

PikaWorkerThread::~PikaWorkerThread() {

}

void PikaWorkerThread::CronHandle() {
  WorkerCronTask t;

  {
  slash::MutexLock l(&mutex_);
  if (cron_tasks_.empty()) {
    return;
  }
  t = cron_tasks_.front();
  cron_tasks_.pop();
  }

  DLOG(INFO) << "Got a WorkerCronTask";
  switch (t.task) {
    case TASK_KILL:
      ClientKill(t.ip_port);
      break;
    case TASK_KILLALL:
      ClientKillAll();
      break;
  }
}

void PikaWorkerThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

void PikaWorkerThread::ClientKill(std::string ip_port) {
  slash::RWLock l(rwlock(), true);
  std::map<int, void*>::iterator iter;
  for (iter = conns()->begin(); iter != conns()->end(); iter++) {
    if (static_cast<PikaConn*>(iter->second)->ip_port() != ip_port) {
      continue;
    }
    DLOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PikaConn*>(iter->second));
    break;
  }
}

void PikaWorkerThread::ClientKillAll() {
  slash::RWLock l(rwlock(), true);
  std::map<int, void*>::iterator iter = conns()->begin();
  while (iter != conns()->end()) {
    DLOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PikaConn*>(iter->second));
    iter = conns()->erase(iter);
  }
}


};
