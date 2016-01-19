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
  PikaWorkerThread** pika_worker_thread() {
    return pika_worker_thread_;
  };
  const PikaDispatchThread* pika_dispatch_thread() {
    return pika_dispatch_thread_;
  };
  const PikaBinlogReceiverThread* pika_binlog_receiver_thread() {
    return pika_binlog_receiver_thread_;
  }


  void Start();
  slash::Mutex mutex_;

private:
  int port_;
  PikaWorkerThread* pika_worker_thread_[PIKA_MAX_WORKER_THREAD_NUM];
  PikaDispatchThread* pika_dispatch_thread_;

  PikaBinlogReceiverThread* pika_binlog_receiver_thread_;


  PikaServer(PikaServer &ps);
  void operator =(const PikaServer &ps);
};
#endif
