#include <glog/logging.h>
#include "pika_dispatch_thread.h"
#include "pika_client_conn.h"
#include "pika_server.h"

extern PikaServer* g_pika_server;

PikaDispatchThread::PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  DispatchThread::DispatchThread(port, work_num, reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread), cron_interval) {
}

PikaDispatchThread::~PikaDispatchThread() {

}

void PikaDispatchThread::CronHandle() {
  uint64_t server_querynum = 0;
  uint64_t server_current_qps = 0;
  for (int i = 0; i < work_num(); i++) {
    slash::RWLock(&(((PikaWorkerThread**)worker_thread())[i]->rwlock_), false);
    server_querynum += ((PikaWorkerThread**)worker_thread())[i]->thread_querynum();
    server_current_qps += ((PikaWorkerThread**)worker_thread())[i]->last_sec_thread_querynum();
  }

  // Should not here, Just for test, remove to info cmd later
  server_current_qps += g_pika_server->pika_binlog_receiver_thread()->thread_querynum();
  server_current_qps += g_pika_server->pika_binlog_receiver_thread()->last_sec_thread_querynum();


//  DLOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
  LOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
}

bool PikaDispatchThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }
  if (ClientNum() >= 1000) {
    DLOG(INFO) << "Max connections reach, Deny new comming: " << ip;
    return false;
  }
  DLOG(INFO) << "ip: " << ip;
  return true;
}

int PikaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((PikaWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
