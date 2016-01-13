#ifndef PIKA_CONN_H_
#define PIKA_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"

class Thread;

namespace pika {

class PikaWorkerThread;

class PikaConn: public RedisConn {
public:
  explicit PikaConn(int fd, std::string ip_port, Thread *thread);
  virtual int DealMessage();
private:
  PikaWorkerThread* pika_thread_;
};

};
#endif
