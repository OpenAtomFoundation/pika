#include "pika_dispatch_thread.h"
#include "pika_conn.h"

namespace pika {
PikaDispatchThread::PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  DispatchThread::DispatchThread(port, work_num, reinterpret_cast<WorkerThread<PikaConn>**>(pika_worker_thread), cron_interval) {
}

PikaDispatchThread::~PikaDispatchThread() {

}

void PikaDispatchThread::CronHandle() {
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(((PikaWorkerThread**)worker_thread())[i]->rwlock(), false);
    server_querynum += ((PikaWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((PikaWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }
  DLOG(INFO) << "ServerQuerynum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
}

bool PikaDispatchThread::AccessHandle(std::string& ip_port) {
  if (ClientNum() == 1) {
    DLOG(INFO) << "Max connections reach, Deny new comming: " << ip_port;
    return false;
  }
  DLOG(INFO) << "ip_port: " << ip_port;
  return true;
}

int PikaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((PikaWorkerThread**)worker_thread())[i]->ThreadClientNum();
    worker_thread()[i]->conns();
  }
  return num;
}

};
