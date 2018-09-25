#ifndef MIGRATOR_THREAD_H_
#define MIGRATOR_THREAD_H_

#include <iostream>

#include "pink/include/redis_cli.h"

#include "pika_sender.h"
#include "conf.h"

class MigratorThread : public pink::Thread {
 public:
  MigratorThread(void *db, std::vector<PikaSender *> *senders, char type, int thread_num) :
      db_(db),
      senders_(senders),
      type_(type),
      thread_num_(thread_num),
      thread_index_(0),
      num_(0) {
  }

  int64_t num() {
    slash::MutexLock l(&num_mutex_);
    return num_;
  }
  
  virtual ~ MigratorThread();
  bool should_exit_;

 private:
  void PlusNum() {
    slash::MutexLock l(&num_mutex_);
    ++num_;
  }

  void MigrateStringsDB();
  virtual void *ThreadMain();

 private:
  void* db_;

  std::vector<PikaSender *> *senders_;
  char type_;
  int thread_num_;
  int thread_index_;

  void MigrateDB(const char type);
  void DispatchKey(const std::string &key);

  int64_t num_;
  slash::Mutex num_mutex_;
};

#endif

