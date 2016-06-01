#ifndef MASTER_CONN_H_
#define MASTER_CONN_H_

#include "redis_conn.h"
#include "pink_thread.h"
#include "pika_command.h"

class BinlogReceiverThread;

class MasterConn: public pink::RedisConn {
public:
  MasterConn(int fd, std::string ip_port, pink::Thread *thread);
  virtual ~MasterConn();
  virtual int DealMessage();
private:
  BinlogReceiverThread* self_thread_;
  void RestoreArgs();
  std::string raw_args_;
};

#endif
