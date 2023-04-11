#ifndef PIKA_SENDER_H_
#define PIKA_SENDER_H_

#include <chrono>
#include <iostream>
#include <queue>
#include <thread>
#include "nemo.h"
#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class PikaSender : public net::Thread {
 public:
  PikaSender(nemo::Nemo* db, std::string ip, int64_t port, std::string password);
  virtual ~PikaSender();
  void LoadKey(const std::string& cmd);
  void Stop() {
    should_exit_ = true;
    keys_mutex_.Lock();
    rsignal_.Signal();
    keys_mutex_.Unlock();
  }
  int64_t elements() { return elements_; }

  void SendCommand(std::string& command, const std::string& key);
  int QueueSize() {
    slash::MutexLock l(&keys_mutex_);
    int len = keys_queue_.size();
    return len;
  }
  void ConnectRedis();

 private:
  net::PinkCli* cli_;
  slash::CondVar rsignal_;
  slash::CondVar wsignal_;
  nemo::Nemo* db_;
  slash::Mutex keys_mutex_;
  std::queue<std::string> keys_queue_;
  std::string expire_command_;
  std::string ip_;
  int port_;
  std::string password_;
  bool should_exit_;
  int64_t elements_;

  virtual void* ThreadMain();
};

#endif
