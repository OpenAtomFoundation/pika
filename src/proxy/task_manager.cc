#include <cassert>
#include <unique_ptr.h>
#include <condition_variable>

#include "task_manager.h"
#include "pstd/log.h"
#include "util.h"
#include "pikiwidb.h"

namespace pikiwidb {

const size_t TaskManager::kMaxWorkers = 8;

bool TaskManager::SetWorkerNum(size_t num) {
  if (num <= 1) return true;
  if (state_ != State::kNone) {
    ERROR("can only called before application run");
    return false;
  }
  if (!worker_loops_.empty()) {
    ERROR("can only called once, not empty loops size: {}", worker_loops_.size());
    return false;
  }
  worker_num_.store(num);
  worker_threads_.reserve(num);
  worker_loops_.reserve(num);
  return true;
}

bool TaskManager::Init() {
  auto f = [this] { return ChooseNextWorkerEventLoop(); };
  
  base_.Init();
  INFO("base loop {} {}, g_baseLoop {}", base_.GetName(), static_cast<void*>(&base_)),
    static_cast<void*>(pikiwidb::EventLoop::Self());

  return true;
}

void TaskManager::Run(int ac, char* av[]) {
  assert(state_ == State::kNone);
  INFO("Process {} starting...", name_);
  
  StartWorkers();
  base_.Run();

  for (auto& w : worker_threads_) {
    if (w.joinable()) {
      w.join();
    }
  }

  worker_threads_.clear();
  
  INFO("Process {} stopped, goodbye...", name_);
}

void TaskManager::Exit() {
  state_ = State::kStopped;

  BaseLoop()->Stop();
  for (const auto& worker_loops : worker_loops_) {
    EventLoop* loop = worker_loops.get();
    loop->Stop();
  }
}

bool TaskManager::IsExit() const { return state_ == State::kStopped; }

EventLoop* TaskManager::BaseLoop() { return &base_; }

EventLoop* TaskManager::ChooseNextWorkerEventLoop() {
  if (worker_loops_.empty())
    return BaseLoop();
  
  auto& loop = worker_loops_[current_work_loop_++ % worker_loops_.size()];
  return loop.get();
}

void TaskManager::StartWorkers() {
  assert(state_ == State::kNone);
  
  size_t index = 1;
  while (worker_loops_.size() < worker_num_) {
    std::unique_ptr<EventLoop> loop = std::make_unique<EventLoop>();
    if (!name_.empty()) {
      loop->SetName(name_ + std::to_string(index++));
      INFO("loop {}, name {}", static_cast<void*>(loop.get()), loop->GetName().c_str());
    }
    worker_loops_.push_back(std::move(loop));
  }

  for (index = 0; index < worker_loops_.size(); ++index) {
    EventLoop* loop = worker_loops_[index].get();
    std::thread t([loop]() {
      loop->Init();
      loop->Run();
    });
    INFO("thread {}, thread loop {}, loop name {}", index, static_cast<void*>(loop), loop->GetName().c_str());
    worker_threads_.push_back(std::move(t));
  }

  state_ = State::kStarted;

  TaskMutex_.reserve(worker_num_);
  TaskCond_.reserve(worker_num_);
  TaskQueue_.reserve(worker_num_);
  
  for (size_t index = 0; index < worker_num_; ++index) {
    TaskMutex_.emplace_back(std::make_unique<std::mutex>());
    TaskCond_.emplace_back(std::make_unique<std::condition_variable>());
    TaskQueue_.emplace_back();
    
    std::thread t([this, index]() {
      while (TaskRunning_) {
        std::unique_lock lock(*TaskMutex_[index]);
        while (TaskQueue_[index].empty()) {
          if (!TaskRunning_) break;
          TaskCond_[index]->wait(lock);
        }
        if (!TaskRunning_) break;
        auto task = TaskQueue_[index].front();

        switch (task->GetTaskType()) {
        case ProxyBaseCmd::TaskType::kExecute:
          task->Execute();
          break;
        case ProxyBaseCmd::TaskType::kCallback: 
          task->CallBack();
          g_pikiwidb->PushWriteTask(task->Client());
          // return  DoCmd 之后有无返回，client 是否有 eventloop 维护
          break;
        default:
          ERROR("unsupported task type...");
          break;
        } 
        
        TaskQueue_[index].pop_front();
      }
      INFO("worker write thread {}, goodbye...", index);
    });

    INFO("worker write thread {}, starting...", index);
  }
  
}

void TaskManager::SetName(const std::string& name) { name_ = name; }

void TaskManager::Reset() {
  state_ = State::kNone;
  BaseLoop()->Reset();
}

void TaskManager::PushTask(std::shared_ptr<ProxyBaseCmd> task) {
  auto pos = (++t_counter_) % worker_num_;
  std::unique_lock lock(*TaskMutex_[pos]);
  
  TaskQueue_[pos].emplace_back(task);
  TaskCond_[pos]->notify_one();
}

void TaskManager::Exit() {
  TaskRunning_ = false;

  int i = 0;
  for (auto& cond : TaskCond_) {
    std::unique_lock lock(*TaskMutex_[i++]);
    cond->notify_all();
  }
  for (auto& wt : TaskThreads_) {
    if (wt.joinable()) {
      wt.join();
    }
  }

  TaskThreads_.clear();
  TaskCond_.clear();
  TaskQueue_.clear();
  TaskMutex_.clear();
}

}