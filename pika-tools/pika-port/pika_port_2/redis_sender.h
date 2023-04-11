#ifndef REDIS_SENDER_H_
#define REDIS_SENDER_H_

#include <atomic>
#include <chrono>
#include <iostream>
#include <queue>
#include <thread>

#include "nemo.h"
#include "net/include/bg_thread.h"
#include "net/include/net_cli.h"
#include "net/include/redis_cli.h"

class RedisSender : public net::Thread {
 public:
  RedisSender(int id, std::string ip, int64_t port, std::string password);
  virtual ~RedisSender();
  void Stop(void);
  int64_t elements() { return elements_; }

  void SendRedisCommand(const std::string& command);

 private:
  int SendCommand(std::string& command);
  void ConnectRedis();

 private:
  int id_;
  net::PinkCli* cli_;
  slash::CondVar rsignal_;
  slash::CondVar wsignal_;
  slash::Mutex commands_mutex_;
  std::queue<std::string> commands_queue_;
  std::string ip_;
  int port_;
  std::string password_;
  bool should_exit_;
  int64_t elements_;
  std::atomic<time_t> last_write_time_;

  virtual void* ThreadMain();
};

#endif
