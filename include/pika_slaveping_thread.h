#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include "pink_thread.h"
#include "slash_mutex.h"
#include "redis_cli.h"


class PikaSlavepingThread : public pink::Thread {
public:
  PikaSlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
    cli_ = new pink::RedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~PikaSlavepingThread() {
    should_exit_ = true;
    pthread_join(thread_id(), NULL);
    delete cli_;
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
