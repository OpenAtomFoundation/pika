#ifndef PIKA_TRYSYNC_THREAD_H_
#define PIKA_TRYSYNC_THREAD_H_

#include "pink_thread.h"

class PikaTrysyncThread : public pink::Thread {
public:
  PikaTrysyncThread() {
	};
  virtual ~PikaTrysyncThread() {
    should_exit_ = true;
    pthread_join(thread_id(), NULL);
    DLOG(INFO) << " Trysync thread " << pthread_self() << " exit!!!";
	};
	

private:
  int sockfd_;
  int64_t sid_;
	bool Init();
  bool Connect(const std::string& master_ip, int master_port);
  bool Send();
  bool RecvProc();

  virtual void* ThreadMain();

};

#endif
