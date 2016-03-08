#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"
#include "pink_thread.h"
#include "pika_command.h"


class PikaWorkerThread;

class PikaClientConn: public pink::RedisConn {
public:
  PikaClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaClientConn();
  virtual int DealMessage();

private:
  PikaWorkerThread* self_thread_;
  std::string DoCmd(const std::string& opt);
  std::string RestoreArgs();
};

#endif
