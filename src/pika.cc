#include "pika_server.h"

int main()
{
  // pika::PikaWorkerThread *pikaWorkerThread[1];
  // for (int i = 0; i < 1; i++) {
    // pikaWorkerThread[i] = new pika::PikaWorkerThread(1000);
  // }
  // pika::PikaDispatchThread *pikaDispatchThread = new pika::PikaDispatchThread(9211, 1, pikaWorkerThread, 3000);

  // FLAGS_minloglevel = 0;
  // FLAGS_alsologtostderr = true;
  // FLAGS_log_dir = "./logs";
  // ::google::InitGoogleLogging("pika");
  // pikaDispatchThread->StartThread();
  PikaServer* pikaServer = new PikaServer(9211);
  pikaServer->Start();

  return 0;
}
