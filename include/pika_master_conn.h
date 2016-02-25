#ifndef PIKA_MASTER_CONN_H_
#define PIKA_MASTER_CONN_H_

#include "redis_conn.h"
#include "pink_thread.h"

//class pink::Thread;
class PikaBinlogReceiverThread;

class PikaMasterConn: public pink::RedisConn {
public:
  PikaMasterConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~PikaMasterConn();
  virtual int DealMessage();
private:
  PikaBinlogReceiverThread* self_thread_;
};

#endif
