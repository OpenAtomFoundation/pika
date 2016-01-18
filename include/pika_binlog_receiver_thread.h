#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include <glog/logging.h>

#include "holy_thread.h"
#include "pika_master_conn.h"

namespace pika {
class PikaBinlogReceiverThread : public pink::HolyThread<PikaMasterConn>
{
public:
  PikaBinlogReceiverThread(int port);
  virtual ~PikaBinlogReceiverThread();
  virtual bool AccessHandle(std::string& ip_port);
};
};
#endif
