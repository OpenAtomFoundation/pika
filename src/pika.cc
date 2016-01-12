#include "dispatch_thread.h"
#include "pika_worker_thread.h"

int main()
{
  pika::PikaWorkerThread *pikaWorkerThread[1];
  for (int i = 0; i < 1; i++) {
    pikaWorkerThread[i] = new pika::PikaWorkerThread(1000);
  }
   Thread *t = new DispatchThread<pika::PikaConn>(9211, 1, reinterpret_cast<WorkerThread<pika::PikaConn> **>(pikaWorkerThread), 3000);

  FLAGS_minloglevel = 0;
  FLAGS_alsologtostderr = true;
  FLAGS_log_dir = "./logs";
  ::google::InitGoogleLogging("pika");
  t->StartThread();


  sleep(1000);
  return 0;
}
