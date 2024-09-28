#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <thread>

#include "client.h"
#include "proxy_base_cmd.h"
#include "cmd_thread_pool.h"
#include "net/event_loop.h"
#include "base_cmd.h"
#include "cmd_table_manager.h"

namespace pikiwidb {


class TaskManager {
public:
  TaskManager() = default;
  ~TaskManager() = default;
  
  static size_t GetMaxWorkerNum() { return kMaxWorkers; }
  
  bool Init();
  void Run(int argc, char* argv[]);
  void Exit();
  void PushTask(std::shared_ptr<ProxyBaseCmd> task);
  bool IsExit() const;
  void SetName(const std::string& name);

  EventLoop* BaseLoop();
  
  EventLoop* ChooseNextWorkerEventLoop();
  
  bool SetWorkerNum(size_t n);

  void Reset();
    
protected:
  virtual void StartWorkers();
  
  static const size_t kMaxWorkers;
  
  std::string name_;
    
  EventLoop base_;
  
  std::atomic<size_t> worker_num_{0};
  std::vector<std::thread> worker_threads_;
  std::vector<std::unique_ptr<EventLoop>> worker_loops_;
  mutable std::atomic<size_t> current_work_loop_{0};
  
  enum class State {
    kNone,
    kStarted,
    kStopped,
  };
  std::atomic<State> state_{State::kNone};

  pikiwidb::CmdTableManager cmd_table_manager_;

private:
  std::vector<std::thread> TaskThreads_;
  std::vector<std::unique_ptr<std::mutex>> TaskMutex_;
  std::vector<std::unique_ptr<std::condition_variable>> TaskCond_;
  std::vector<std::deque<std::shared_ptr<ProxyBaseCmd>>> TaskQueue_;
  
  std::atomic<uint64_t> t_counter_ = 0;
  bool TaskRunning_ = true;
};

} // namespace pikiwidb