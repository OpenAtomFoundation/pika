#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include "holy_thread.h"
#include "pika_master_conn.h"
#include "pika_command.h"

class PikaBinlogReceiverThread : public pink::HolyThread<PikaMasterConn>
{
public:
  PikaBinlogReceiverThread(int port);
  virtual ~PikaBinlogReceiverThread();
  virtual bool AccessHandle(const std::string& ip_port);
  void KillAll();

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }
private:
  std::unordered_map<std::string, Cmd*> cmds_;
};
#endif
