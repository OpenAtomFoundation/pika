#include "pika_worker_thread.h"

namespace pika {
PikaWorkerThread::PikaWorkerThread(int cron_interval):
  WorkerThread::WorkerThread(cron_interval) {
}

PikaWorkerThread::~PikaWorkerThread() {

}
void PikaWorkerThread::CronHandle() {
  DLOG(INFO) << "======PikaWorkerThread Cron======";
}
};
