#include "pika_server.h"

PikaServer::PikaServer(int port) :
  port_(port) {

  for (int i = 0; i < PIKA_MAX_WORKER_THREAD_NUM; i++) {
    pikaWorkerThread_[i] = new PikaWorkerThread(1000);
  }

  pikaDispatchThread_ = new PikaDispatchThread(port_, PIKA_MAX_WORKER_THREAD_NUM, pikaWorkerThread_, 3000);
  pikaBinlogReceiverThread_ = new PikaBinlogReceiverThread(port_ + 100);

  FLAGS_minloglevel = 0;
  FLAGS_alsologtostderr = true;
  FLAGS_log_dir = "./logs";
  ::google::InitGoogleLogging("pika");
}

void PikaServer::Start() {
  pikaDispatchThread_->StartThread();
  pikaBinlogReceiverThread_->StartThread();
  while(1) {
  };
}
