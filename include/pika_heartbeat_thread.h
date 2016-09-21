#ifndef PIKA_HEARTBEAT_THREAD_H_
#define PIKA_HEARTBEAT_THREAD_H_

#include "holy_thread.h"
#include "pika_heartbeat_conn.h"

class PikaHeartbeatThread : public pink::HolyThread<PikaHeartbeatConn>
{
public:
  PikaHeartbeatThread(std::string &ip, int port, int cron_interval = 0);
  PikaHeartbeatThread(std::set<std::string> &ip, int port, int cron_interval = 0);
  virtual ~PikaHeartbeatThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip_port);

  bool FindSlave(int fd); //hb_fd
};
#endif
