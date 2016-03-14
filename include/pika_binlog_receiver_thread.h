#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include <queue>

#include "holy_thread.h"
#include "slash_mutex.h"
#include "pika_define.h"
#include "pika_master_conn.h"
#include "pika_command.h"

class PikaBinlogReceiverThread : public pink::HolyThread<PikaMasterConn>
{
public:
  PikaBinlogReceiverThread(int port, int cron_interval = 0);
  virtual ~PikaBinlogReceiverThread();
  virtual void CronHandle();
  virtual bool AccessHandle(std::string& ip);
  void KillBinlogSender();

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
  slash::Mutex mutex_; // protect cron_task_
  void AddCronTask(WorkerCronTask task);
  void KillAll();
  std::queue<WorkerCronTask> cron_tasks_;

  std::unordered_map<std::string, Cmd*> cmds_;
  uint64_t thread_querynum_;
  uint64_t last_sec_thread_querynum_;
};
#endif
