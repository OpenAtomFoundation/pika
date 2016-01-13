#include "pika_dispatch_thread.h"
#include "pika_conn.h"

namespace pika {
PikaDispatchThread::PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval) :
  DispatchThread::DispatchThread(port, work_num, reinterpret_cast<WorkerThread<PikaConn>**>(pika_worker_thread), cron_interval) {
}

PikaDispatchThread::~PikaDispatchThread() {

}

void PikaDispatchThread::CronHandle() {
  DLOG(INFO) << "======PikaDispatchThread Cron======";
}

bool PikaDispatchThread::AccessHandle(std::string& ip_port) {
  DLOG(INFO) << "======Come in AccessHandle======";
  DLOG(INFO) << "ip_port: " << ip_port;
  return true;
}

};
