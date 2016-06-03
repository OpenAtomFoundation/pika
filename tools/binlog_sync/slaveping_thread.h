#ifndef SLAVEPING_THREAD_H_
#define SLAVEPING_THREAD_H_

#include "pink_thread.h"
#include "slash_mutex.h"
#include "redis_cli.h"


class SlavepingThread : public pink::Thread {
public:
  SlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
    cli_ = new pink::RedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~SlavepingThread() {
    should_exit_ = true;
    pthread_join(thread_id(), NULL);
    delete cli_;
    DLOG(INFO) << " Slaveping thread " << pthread_self() << " exit!!!";
	};

  pink::Status Send();
  pink::Status RecvProc();

private:
  int64_t sid_;
  bool is_first_send_;

  int sockfd_;
  pink::RedisCli *cli_;

  virtual void* ThreadMain();

};

#endif
