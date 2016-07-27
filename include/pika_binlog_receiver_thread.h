#ifndef PIKA_BINLOG_RECEIVER_THREAD_H_
#define PIKA_BINLOG_RECEIVER_THREAD_H_

#include <queue>

#include "holy_thread.h"
#include "slash_mutex.h"
#include "env.h"
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

  void ResetThreadQuerynum() {
    slash::RWLock(&rwlock_, true);
    thread_querynum_ = 0;
    last_thread_querynum_ = 0;
  }

  uint64_t last_sec_thread_querynum() {
    slash::RWLock(&rwlock_, false);
    return last_sec_thread_querynum_;
  }

  uint64_t GetnPlusSerial() {
    return serial_++;
  }

  void PlusThreadQuerynum() {
    slash::RWLock(&rwlock_, true);
    thread_querynum_++;
  }

  void ResetLastSecQuerynum() {
    uint64_t cur_time_ms = slash::NowMicros();
    slash::RWLock(&rwlock_, true);
    last_sec_thread_querynum_ = (thread_querynum_ - last_thread_querynum_) * 1000000 / (cur_time_ms - last_time_us_+1);
    last_time_us_ = cur_time_ms;
    last_thread_querynum_ = thread_querynum_;
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
  uint64_t last_thread_querynum_;
  uint64_t last_time_us_;
  uint64_t last_sec_thread_querynum_;
  uint64_t serial_;
};
#endif
