#ifndef SENDER_H_
#define SENDER_H_

#include <thread>
#include <chrono>
#include <iostream>
#include <queue>
#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"
#include "nemo.h"

class Sender : public pink::Thread {
public:
	Sender(nemo::Nemo *db, std::string ip, int64_t port, std::string password);
	virtual ~Sender();
	void LoadKey(const std::string &cmd);
	void Stop() {
  	  should_exit_ = true;
	}
	int64_t elements() {
      return elements_;
  	}

  	int QueueSize() {
  	  slash::MutexLock l(&keys_mutex_);
  	  int len = keys_queue_.size();
  	  return len;
  	}
private:
	nemo::Nemo *db_;
	slash::Mutex keys_mutex_;
	std::queue<std::string> keys_queue_;
	std::string ip_;
	int port_;
	std::string password_;
	std::string cmd_;
  	bool should_exit_;
  	int64_t elements_;
  	int64_t full_;

	virtual void *ThreadMain();
};

#endif
