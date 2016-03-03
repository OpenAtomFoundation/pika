#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include "pink_thread.h"
#include "slash_mutex.h"

class PikaSlavepingThread : public pink::Thread {
public:
  PikaSlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true),
  is_exit_(false) {
	};
  virtual ~PikaSlavepingThread() {
	};

  void SetExit() {
    slash::MutexLock l(&mutex_);
    is_exit_ = true;
  }

  bool IsExit() {
    slash::MutexLock l(&mutex_);
    bool ret = is_exit_;
    return ret;
  }

private:
  int sockfd_;
  int64_t sid_;
  bool is_first_send_;
  bool is_exit_;
  slash::Mutex mutex_; //protect is_exit_
	bool Init();
  bool Connect(const std::string& master_ip, int master_ping_port);
  bool Send();
  bool RecvProc();

  virtual void* ThreadMain();

};

#endif
