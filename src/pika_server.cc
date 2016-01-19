#include <glog/logging.h>
#include "pika_server.h"

PikaServer::PikaServer(int port) :
  port_(port) {

  for (int i = 0; i < PIKA_MAX_WORKER_THREAD_NUM; i++) {
    pika_worker_thread_[i] = new PikaWorkerThread(1000);
  }

  pika_dispatch_thread_ = new PikaDispatchThread(port_, PIKA_MAX_WORKER_THREAD_NUM, pika_worker_thread_, 3000);
  pika_binlog_receiver_thread_ = new PikaBinlogReceiverThread(port_ + 100);
}

void PikaServer::Start() {
  pika_dispatch_thread_->StartThread();
  pika_binlog_receiver_thread_->StartThread();
  mutex_.Lock();
  mutex_.Lock();
  DLOG(INFO) << "Goodbye...";
  mutex_.Unlock();
}
