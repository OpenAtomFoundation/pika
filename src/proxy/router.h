#pragma once

#include <chrono>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>
#include <memory>
#include "proxy_base_cmd.h"
#include "brpc_redis.h"

class TimerWheel {
public:
  using Task = std::function<void()>;
  
  explicit TimerWheel(size_t wheel_size, int interval_ms) 
    : wheel_size_(wheel_size),
    interval_ms_(interval_ms),
    wheel_(wheel_size),
    current_index_(0) {}

  ~TimerWheel() {
    Stop();
  }

  void Start() {
    if (running_) return;
    running_ = true;
    thread_ = std::thread([this]() {
      while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        Tick();
      }
    });
    thread_.detach();
  }

  void Stop() {
    if (!running_) return;
    running_ = false;
    if (thread_.joinable()) thread_.join();
  }

  void AddTask(int timeout_ms, Task task) {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t ticks = timeout_ms / interval_ms_;
    size_t index = (current_index_ + ticks) % wheel_size_;
    size_t allindex = index;
    for (size_t i = 1; allindex < wheel_size_; i++) {
      allindex = index * i;
      if (allindex >= wheel_size_) break;
      wheel_[allindex].push_back(task);
    }
  }
  
private:
  void Tick() {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& tasks = wheel_[current_index_];
    for (const auto& task : tasks) {
      task();
    }
    current_index_ = (current_index_ + 1) % wheel_size_;
  }
    
private:
  size_t wheel_size_;
  int interval_ms_;
  std::vector<std::list<Task>> wheel_;
  size_t current_index_;
  bool running_ = false;
  std::thread thread_;
  std::mutex mutex_;
};

namespace pikiwidb {
class Router {
public:
  static Router& Instance();
  
  Router(const Router&) = delete;
  void operator=(const Router&) = delete;
  ~Router();
  
  void Init();
  void forward(std::shared_ptr<ProxyBaseCmd> task);

private:
  Router() = default;
  std::atomic<uint64_t> t_counter_ = 0;
  std::atomic<size_t> brpc_redis_num_{0};
  std::vector<BrpcRedis> brpc_redis_;
  std::hash<std::string> hasher_;

  TimerWheel* timer_wheel_;
};  

#define ROUTER Router::Instance()

} // namespace pikiwidb