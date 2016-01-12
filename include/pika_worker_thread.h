#ifndef PIKA_WORKER_THREAD_H_
#define PIKA_WORKER_THREAD_H_

#include <glog/logging.h>

#include "worker_thread.h"
#include "pika_conn.h"

namespace pika {
class PikaWorkerThread : public WorkerThread<PikaConn>
{
public:
  PikaWorkerThread(int cron_interval = 0);
  virtual ~PikaWorkerThread();
  virtual void CronHandle();
};
};
#endif
