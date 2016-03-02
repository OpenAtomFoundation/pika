#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include "simple_thread.h"

class PikaSlavepingThread : public pink::SimpleThread {
public:
  PikaSlavepingThread(int64_t sid) : sid_(sid),
  is_first_send_(true) {
	};
  virtual ~PikaSlavepingThread() {
	};
	

private:
  int sockfd_;
  int64_t sid_;
  bool is_first_send_;
	bool Init();
  bool Connect(const std::string& master_ip, int master_ping_port);
  bool Send();
  bool RecvProc();

  virtual void* ThreadMain();

};

#endif
