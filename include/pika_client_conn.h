#ifndef PIKA_CLIENT_CONN_H_
#define PIKA_CLIENT_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"
#include "pink_thread.h"


class PikaWorkerThread;

class PikaClientConn: public pink::RedisConn {
public:
  PikaClientConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaClientConn();
  virtual int DealMessage();
private:
  PikaWorkerThread* pika_thread_;
  int DoCmd(const std::string& opt, const std::string& raw_args, std::string& ret);
  std::string RestoreArgs();
};

#endif
