#ifndef MIGRATOR_THREAD_H_
#define MIGRATOR_THREAD_H_

#include <iostream>

#include "net/include/redis_cli.h"

#include "conf.h"
#include "pika_sender.h"

class MigratorThread : public net::Thread {
 public:
  MigratorThread(void* db, std::vector<PikaSender*>* senders, int type, int thread_num)
      : db_(db),
        should_exit_(false),
        senders_(senders),
        type_(type),
        thread_num_(thread_num),
        thread_index_(0),
        num_(0) {}

  virtual ~MigratorThread();

  int64_t num() {
    pstd::MutexLock l(&num_mutex_);
    return num_;
  }

  void Stop() { should_exit_ = true; }

 private:
  void PlusNum() {
    pstd::MutexLock l(&num_mutex_);
    ++num_;
  }

  void DispatchKey(const std::string& command, const std::string& key = "");

  void MigrateDB();
  void MigrateStringsDB();
  void MigrateListsDB();
  void MigrateHashesDB();
  void MigrateSetsDB();
  void MigrateZsetsDB();

  virtual void* ThreadMain();

 private:
  void* db_;
  bool should_exit_;

  std::vector<PikaSender*>* senders_;
  int type_;
  int thread_num_;
  int thread_index_;

  int64_t num_;
  pstd::Mutex num_mutex_;
};

#endif
