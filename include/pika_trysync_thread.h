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
  virtual ~PikaTrysyncThread();

private:
  int sockfd_;
  int64_t sid_;
  pink::RedisCli *cli_;

  bool Send();
  bool RecvProc();
  void PrepareRsync();
  bool TryUpdateMasterOffset();

  virtual void* ThreadMain();

};

#endif
