#ifndef PARSE_THREAD_H_
#define PARSE_THREAD_H_

#include "nemo.h"
#include "slash/include/slash_mutex.h"
#include "pink/include/pink_thread.h"
#include "pink/include/redis_cli.h"
#include "sender_thread.h"

class ParseThread : public pink::Thread {
public:
  ParseThread(nemo::Nemo *db, SenderThread *sender, int full = 100) :
    db_(db),
    sender_(sender),
    task_r_cond_(&task_mutex_),
    task_w_cond_(&task_mutex_),
    full_(full),
    num_(0) {
  }

  virtual ~ParseThread();
  void Stop();
  void Schedul(const std::string &key, char type);
  int64_t num() {
    slash::MutexLock l(&num_mutex_);
    return num_;
  }
  bool should_exit_;
private:
  // prase key and generate cmd
  void SetTTL(const std::string &key, int64_t ttl);
  void ParseKey(const std::string &key,char type);
  void ParseKKey();
  void ParseHKey(const std::string &key);
  void ParseSKey(const std::string &key);
  void ParseZKey(const std::string &key);
  void ParseLKey(const std::string &key);

  struct Task {
    std::string key;
    char type;
    Task(std::string _key, char _type)
      : key(_key), type(_type) {}
  };

  nemo::Nemo *db_;
  SenderThread *sender_;

  std::deque<Task> task_queue_;
  slash::Mutex task_mutex_;
  slash::CondVar task_r_cond_;
  slash::CondVar task_w_cond_;

  size_t full_;
  int64_t num_;
  slash::Mutex num_mutex_;

  void PlusNum() {
    slash::MutexLock l(&num_mutex_);
    ++num_;
  }

  virtual void *ThreadMain();
};
#endif
