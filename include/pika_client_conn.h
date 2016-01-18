#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"

class Thread;

namespace pika {

class PikaWorkerThread;

class PikaClientConn: public RedisConn {
public:
  PikaClientConn(int fd, std::string ip_port, Thread *thread);
  virtual ~PikaClientConn();
  virtual int DealMessage();
private:
  PikaWorkerThread* pika_thread_;
};

};
#endif
