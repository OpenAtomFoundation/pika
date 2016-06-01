#ifndef TRYSYNC_THREAD_H_
#define TRYSYNC_THREAD_H_

#include "pink_thread.h"
#include "redis_cli.h"

class TrysyncThread : public pink::Thread {
public:
  TrysyncThread() {
    cli_ = new pink::RedisCli();
    cli_->set_connect_timeout(1500);
	};
  virtual ~TrysyncThread();

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
