#ifndef PIKA_TRYSYNCSLOT_THREAD_H_
#define PIKA_TRYSYNCSLOT_THREAD_H_

#include "pink_thread.h"
#include "redis_cli.h"

class PikaTrysyncSlotThread : public pink::Thread {
public:
  PikaTrysyncSlotThread() {
    cli_ = new pink::RedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~PikaTrysyncSlotThread();

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
