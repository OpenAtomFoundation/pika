#ifndef PIKA_DISPATCH_THREAD_H_
#define PIKA_DISPATCH_THREAD_H_

#include <glog/logging.h>

#include "pika_worker_thread.h"
#include "dispatch_thread.h"
#include "pika_client_conn.h"

namespace pika {
class PikaDispatchThread : public pink::DispatchThread<PikaClientConn>
{
public:
  PikaDispatchThread(int port, int work_num, PikaWorkerThread** pika_worker_thread, int cron_interval);
  virtual ~PikaDispatchThread();
  virtual void CronHandle();
  virtual bool AccessHandle(const std::string& ip_port);

  int ClientNum();
};
};
#endif
