#ifndef PIKA_WORKER_THREAD_H_
#define PIKA_WORKER_THREAD_H_

#include <glog/logging.h>
#include <queue>

#include "worker_thread.h"
#include "pika_define.h"
#include "slash_mutex.h"
#include "pika_conn.h"

namespace pika {
class PikaWorkerThread : public WorkerThread<PikaConn>
{
public:
  PikaWorkerThread(int cron_interval = 0);
  virtual ~PikaWorkerThread();
  virtual void CronHandle();

  void AddCronTask(WorkerCronTask task);

private:
  slash::Mutex mutex_; // protect cron_task_
  std::queue<WorkerCronTask> cron_tasks_;

  void ClientKill(std::string ip_port);
  void ClientKillAll();
};
};
#endif
