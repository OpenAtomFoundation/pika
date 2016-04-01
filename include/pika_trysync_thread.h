#ifndef PIKA_TRYSYNC_THREAD_H_
#define PIKA_TRYSYNC_THREAD_H_

#include "pink_thread.h"
#include "redis_cli.h"

class PikaTrysyncThread : public pink::Thread {
public:
  PikaTrysyncThread() {
    cli_ = new pink::RedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~PikaTrysyncThread() {
    should_exit_ = true;
    pthread_join(thread_id(), NULL);
    delete cli_;
    DLOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
	};

private:
  int sockfd_;
  int64_t sid_;
  pink::RedisCli *cli_;

  bool Send();
  bool RecvProc();

  virtual void* ThreadMain();

};

#endif
