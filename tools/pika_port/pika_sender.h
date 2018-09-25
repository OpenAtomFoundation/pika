#ifndef PIKA_SENDER_H_
#define PIKA_SENDER_H_

#include <thread>
#include <chrono>
#include <iostream>
#include <queue>

#include "pink/include/bg_thread.h"
#include "pink/include/pink_cli.h"
#include "pink/include/redis_cli.h"

#include "conf.h"

class PikaSender : public pink::Thread {
public:
	PikaSender(void *db, std::string ip, int64_t port, std::string password);
	virtual ~PikaSender();
	void LoadKey(const std::string &cmd);
	void Stop() {
  	  should_exit_ = true;
	  keys_mutex_.Lock();
	  rsignal_.Signal();
      keys_mutex_.Unlock();
	}
	int64_t elements() {
      return elements_;
  	}

	void SendCommand(std::string &command, const std::string &key);
  	int QueueSize() {
  	  slash::MutexLock l(&keys_mutex_);
  	  int len = keys_queue_.size();
  	  return len;
  	}
	void ConnectRedis();
private:
	pink::PinkCli *cli_;
	slash::CondVar rsignal_;
	slash::CondVar wsignal_;
	void *db_;
	slash::Mutex keys_mutex_;
	std::queue<std::string> keys_queue_;
	std::string expire_command_;
	std::string ip_;
	int port_;
	std::string password_;
  	bool should_exit_;
  	int64_t elements_;

	virtual void *ThreadMain();
};

#endif
