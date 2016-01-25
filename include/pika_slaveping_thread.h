#ifndef PIKA_SLAVEPING_THREAD_H_
#define PIKA_SLAVEPING_THREAD_H_

#include "simple_thread.h"

class PikaSlavepingThread : public pink::SimpleThread {
public:
  PikaSlavepingThread() {
	};
  virtual ~PikaSlavepingThread() {
	};
	

private:
  int sockfd_;
	bool Init();
  bool Connect(const std::string& master_ip, int master_ping_port);
  bool Send();
  bool RecvProc();

  virtual void* ThreadMain();

};

#endif
