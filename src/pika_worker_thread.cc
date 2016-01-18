#include "pika_worker_thread.h"

namespace pika {
PikaWorkerThread::PikaWorkerThread(int cron_interval):
  WorkerThread::WorkerThread(cron_interval),
  thread_querynum_(0),
  last_sec_thread_querynum_(0) {
}

PikaWorkerThread::~PikaWorkerThread() {

}

void PikaWorkerThread::CronHandle() {
/*
 *  Do statistic work and find timeout client and add them to cron_tasks_ to kill them
 */
  uint64_t last_sec_thread_querynum_t = 0;
  {
  struct timeval now;
  gettimeofday(&now, NULL);
  slash::RWLock l(&rwlock_, false); // Use ReadLock to iterate the conns_
  std::map<int, void*>::iterator iter = conns_.begin();

  while (iter != conns_.end()) {
/*
 *  Statistics
 */
    last_sec_thread_querynum_t += static_cast<PikaClientConn*>(iter->second)->conn_querynum(); //accumulate thread querynums from 0
    static_cast<PikaClientConn*>(iter->second)->set_conn_querynum(0); // clear conn querynums

/*
 *  Find timeout client
 */
    if (now.tv_sec - static_cast<PikaClientConn*>(iter->second)->last_interaction().tv_sec > 30) {
      DLOG(INFO) << "Find Timeout Client: " << static_cast<PikaClientConn*>(iter->second)->ip_port();
      AddCronTask(WorkerCronTask{TASK_KILL, static_cast<PikaClientConn*>(iter->second)->ip_port()});
    }
    iter++;
  }
  }

/*
 * Update statistics
 */
  {
    slash::RWLock l(&rwlock_, true); // Use WriteLock to update thread_querynum_ and last_sec_thread_querynum_
    thread_querynum_ += last_sec_thread_querynum_t;
    last_sec_thread_querynum_ = last_sec_thread_querynum_t;
  }

/*
 *  do crontask
 */
  {
  WorkerCronTask t;
  slash::MutexLock l(&mutex_);

  while(!cron_tasks_.empty()) {
    t = cron_tasks_.front();
    cron_tasks_.pop();
    mutex_.Unlock();
    DLOG(INFO) << "Got a WorkerCronTask";
    switch (t.task) {
      case TASK_KILL:
        ClientKill(t.ip_port);
        break;
      case TASK_KILLALL:
        ClientKillAll();
        break;
    }
    mutex_.Lock();
  }
  }
}

bool PikaWorkerThread::ThreadClientKill(std::string ip_port) {

  if (ip_port == "") {
    AddCronTask(WorkerCronTask{TASK_KILLALL, ""});
  } else {
    if (!FindClient(ip_port)) {
      return false;
    }
    AddCronTask(WorkerCronTask{TASK_KILL, ip_port});
  }
  return true;
}

int PikaWorkerThread::ThreadClientNum() {
  slash::RWLock l(&rwlock_, false);
  return conns_.size();
}

void PikaWorkerThread::AddCronTask(WorkerCronTask task) {
  slash::MutexLock l(&mutex_);
  cron_tasks_.push(task);
}

bool PikaWorkerThread::FindClient(std::string ip_port) {
  slash::RWLock l(&rwlock_, false);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<PikaClientConn*>(iter->second)->ip_port() != ip_port) {
      return true;
    }
  }
  return false;
}

void PikaWorkerThread::ClientKill(std::string ip_port) {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter;
  for (iter = conns_.begin(); iter != conns_.end(); iter++) {
    if (static_cast<PikaClientConn*>(iter->second)->ip_port() != ip_port) {
      continue;
    }
    DLOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PikaClientConn*>(iter->second));
    conns_.erase(iter);
    break;
  }
}

void PikaWorkerThread::ClientKillAll() {
  slash::RWLock l(&rwlock_, true);
  std::map<int, void*>::iterator iter = conns_.begin();
  while (iter != conns_.end()) {
    DLOG(INFO) << "==========Kill Client==============";
    close(iter->first);
    delete(static_cast<PikaClientConn*>(iter->second));
    iter = conns_.erase(iter);
  }
}


};
