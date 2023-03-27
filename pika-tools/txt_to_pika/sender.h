#ifndef SENDER_H_
#define SENDER_H_

#include <thread>
#include <chrono>
#include <iostream>
#include <queue>
#include "slash/include/xdebug.h"
#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class SenderThread : public net::Thread {
public:
	SenderThread(std::string ip, int64_t port, std::string password);
	virtual ~SenderThread();
	void LoadCmd(const std::string &cmd);
	void Stop() {
  	should_exit_ = true;
	  cmd_mutex_.Lock();
	  rsignal_.Signal();
    cmd_mutex_.Unlock();
	}
	int64_t elements() {
    return elements_;
  }

	void SendCommand(std::string &command);
  
  int QueueSize() {
  	  slash::MutexLock l(&cmd_mutex_);
  	  int len = cmd_queue_.size();
  	  return len;
  }
	void ConnectPika();
private:
	net::PinkCli *cli_;
	slash::CondVar rsignal_;
	slash::CondVar wsignal_;
	slash::Mutex cmd_mutex_;
	std::queue<std::string> cmd_queue_;
	std::string ip_;
	int port_;
	std::string password_;
  bool should_exit_;
  int64_t elements_;

	virtual void *ThreadMain();
};

#endif

