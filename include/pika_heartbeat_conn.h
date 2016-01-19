#ifndef PIKA_HEARTBEAT_CONN_H_
#define PIKA_HEARTBEAT_CONN_H_

#include <glog/logging.h>

#include "redis_conn.h"
#include "pink_thread.h"


class PikaHeartbeatThread;

class PikaHeartbeatConn: public pink::RedisConn {
public:
  PikaHeartbeatConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaHeartbeatConn();
  virtual int DealMessage();
private:
  PikaHeartbeatThread* pika_thread_;
};

#endif
