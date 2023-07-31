#ifndef PIKA_ZSET_AUTO_DEL_THREAD_H_
#define PIKA_ZSET_AUTO_DEL_THREAD_H_

#include <atomic>
#include <deque>

#include "storage//storage.h"
#include "net/include/net_thread.h"
#include "pstd/include/pstd_mutex.h"


enum ZsetTaskType {
  ZSET_NO_TASK,
  ZSET_CRON_TASK,
  ZSET_MANUAL_TASK
};

struct ZsetInfo {
  int64_t last_finish_time;
  int64_t last_spend_time;
  int64_t last_all_keys_num;
  int64_t last_del_keys_num;
  bool last_compact_zset_db;
  int current_task_type;
  int64_t current_task_start_time;
  int64_t current_task_spend_time;
  int64_t current_cursor;
};

struct ZsetTaskItem {
  ZsetTaskType task_type;
  int64_t cursor;
  double speed_factor;
  ZsetTaskItem()
      : task_type(ZSET_NO_TASK)
      , cursor(0)
      , speed_factor(1) {}
};

class PikaZsetAutoDelThread : public net::Thread {
 public:
  PikaZsetAutoDelThread();
  ~PikaZsetAutoDelThread();

  void RequestCronTask();
  void RequestManualTask(int64_t cursor, double speed_factor);
  void StopManualTask();
  void GetZsetInfo(ZsetInfo &info);
  int64_t LastFinishCheckAllZsetTime() { return last_finish_check_all_zset_time_; }

 private:
  void CompactZsetDB();
  void TrimAllZsetKeysFinished();
  void WriteZsetAutoDelBinlog(const std::string &key, int start, int end);
  bool BatchTrimZsetKeys(double speed_factor);
  void DoZsetCronTask(double speed_factor);
  void DoZsetManualTask(int64_t cursor, double speed_factor);
  void DoZsetAutoDelTask(ZsetTaskItem &task_item);
  virtual void* ThreadMain();

 private:
  std::atomic<bool> should_exit_;
  std::mutex mutex_;
  std::condition_variable task_cond_;
  std::deque<ZsetTaskItem> task_queue_;

  std::atomic<ZsetTaskType> current_task_type_;
  std::atomic<int64_t> current_cursor_;
  std::atomic<bool> stop_manual_task_;

  int64_t zset_db_keys_num_;
  int64_t auto_del_keys_num_;
  std::atomic<int64_t> start_check_all_zset_time_;
  std::atomic<int64_t> last_finish_check_all_zset_time_;

  // for zset info use
  std::atomic<int64_t> last_spend_time_;
  std::atomic<int64_t> last_all_keys_num_;
  std::atomic<int64_t> last_del_keys_num_;
  std::atomic<bool> last_compact_zset_db_;
};

#endif
