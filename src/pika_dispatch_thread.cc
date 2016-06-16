#include <glog/logging.h>
#include "pika_dispatch_thread.h"
#include "pika_client_conn.h"
#include "pika_server.h"
#include "pika_conf.h"

extern PikaServer* g_pika_server;
extern PikaConf* g_pika_conf;

PikaDispatchThread::PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  DispatchThread::DispatchThread(port, work_num, reinterpret_cast<pink::WorkerThread<PikaClientConn>**>(pika_worker_thread), cron_interval) {
}

PikaDispatchThread::~PikaDispatchThread() {
  LOG(INFO) << "dispatch thread " << thread_id() << " exit!!!";
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
  server_querynum += g_pika_server->pika_binlog_receiver_thread()->thread_querynum();
  server_current_qps += g_pika_server->pika_binlog_receiver_thread()->last_sec_thread_querynum();

  DLOG(INFO) << "ClientNum: " << ClientNum() << " ServerQueryNum: " << server_querynum << " ServerCurrentQps: " << server_current_qps;
}

bool PikaDispatchThread::AccessHandle(std::string& ip) {
  if (ip == "127.0.0.1") {
    ip = g_pika_server->host();
  }

  int client_num = ClientNum();
  if ((client_num >= g_pika_conf->maxconnection() + g_pika_conf->root_connection_num())
      || (client_num >= g_pika_conf->maxconnection() && ip != g_pika_server->host())) {
    LOG(WARNING) << "Max connections reach, Deny new comming: " << ip;
    return false;
  }

  DLOG(INFO) << "new clinet comming, ip: " << ip;
  g_pika_server->incr_accumulative_connections();
  return true;
}

int PikaDispatchThread::ClientNum() {
  int num = 0;
  for (int i = 0; i < work_num(); i++) {
    num += ((PikaWorkerThread**)worker_thread())[i]->ThreadClientNum();
  }
  return num;
}
