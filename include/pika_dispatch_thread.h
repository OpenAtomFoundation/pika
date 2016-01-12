#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include <glog/logging.h>

#include "dispatch_thread.h"
#include "pika_conn.h"

namespace pika {
class PikaWorkerThread;
class PikaDispatchThread : public DispatchThread<PikaConn> 
{
public:
  PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval);
  ~PikaDispatchThread();
  virtual void CronHandle();
};
};
#endif
