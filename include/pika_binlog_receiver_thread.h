#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include "holy_thread.h"
#include "slash_mutex.h"
#include "pika_master_conn.h"
#include "pika_command.h"

class PikaBinlogReceiverThread : public pink::HolyThread<PikaMasterConn>
{
public:
  PikaBinlogReceiverThread(int port);
  virtual ~PikaBinlogReceiverThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
  void KillAll();

  uint64_t thread_querynum() {
    slash::RWLock(&rwlock_, false);
    return thread_querynum_;
  }

  uint64_t last_sec_thread_querynum() {
    slash::RWLock(&rwlock_, false);
    return last_sec_thread_querynum_;
  }

  void PlusThreadQuerynum() {
    slash::RWLock(&rwlock_, true);
    thread_querynum_++;
    last_sec_thread_querynum_++;
  }

  void ResetLastSecQuerynum() {
    slash::RWLock(&rwlock_, true);
    last_sec_thread_querynum_ = 0;
  }

  int32_t ThreadClientNum() {
    slash::RWLock(&rwlock_, false);
    int32_t num = conns_.size();
    return num;
  }

  Cmd* GetCmd(const std::string& opt) {
    return GetCmdFromTable(opt, cmds_);
  }
private:
  std::unordered_map<std::string, Cmd*> cmds_;
  uint64_t thread_querynum_;
  uint64_t last_sec_thread_querynum_;
};
#endif
