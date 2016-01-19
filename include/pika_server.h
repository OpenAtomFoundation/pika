#ifndef PIKA_SERVER_H_
#define PIKA_SERVER_H_

#include "pika_binlog_receiver_thread.h"
#include "pika_dispatch_thread.h"
#include "pika_worker_thread.h"
#include "pika_define.h"

class PikaServer
{
public:
  PikaServer(int port);
  ~PikaServer();

  /*
   * Get & Set 
   */
  int port() {
    return port_;
  };
  PikaWorkerThread** pikaWorkerThread() {
    return pikaWorkerThread_;
  };
  PikaDispatchThread* pikaDispatchThread() {
    return pikaDispatchThread_;
  };
  PikaBinlogReceiverThread* pikaBinlogReceiverThread() {
    return pikaBinlogReceiverThread_;
  }


  void Start();

private:
  int port_;
  PikaWorkerThread* pikaWorkerThread_[PIKA_MAX_WORKER_THREAD_NUM];
  PikaDispatchThread* pikaDispatchThread_;

  PikaBinlogReceiverThread* pikaBinlogReceiverThread_;

  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};
#endif
