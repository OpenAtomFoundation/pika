#ifndef PIKA_SENDER_H_
#define PIKA_SENDER_H_

#include <atomic>
#include <chrono>
#include <iostream>
#include <queue>
#include <thread>

#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

#include "conf.h"

class PikaSender : public net::Thread {
 public:
  PikaSender(std::string ip, int64_t port, std::string password);
  virtual ~PikaSender();
  void LoadKey(const std::string& cmd);
  void Stop();

  int64_t elements() { return elements_; }

  void SendCommand(std::string& command, const std::string& key);
  int QueueSize();
  void ConnectRedis();

 private:
  net::PinkCli* cli_;
  slash::CondVar signal_;
  slash::Mutex keys_mutex_;
  std::queue<std::string> keys_queue_;
  std::string ip_;
  int port_;
  std::string password_;
  std::atomic<bool> should_exit_;
  int32_t cnt_;
  int64_t elements_;

  virtual void* ThreadMain();
};

#endif
