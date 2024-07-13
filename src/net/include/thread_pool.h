// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef NET_INCLUDE_THREAD_POOL_H_
#define NET_INCLUDE_THREAD_POOL_H_

#include <pthread.h>
#include <atomic>
#include <string>
#include <vector>

#include "net/include/net_define.h"
#include "net/include/random.h"
#include "pstd/include/pstd_mutex.h"

namespace net {

using TaskFunc = void (*)(void*);

struct TimeTask {
  uint64_t exec_time;
  TaskFunc func;
  void* arg;
  TimeTask(uint64_t _exec_time, TaskFunc _func, void* _arg) : exec_time(_exec_time), func(_func), arg(_arg) {}
  bool operator<(const TimeTask& task) const { return exec_time > task.exec_time; }
};

class ThreadPool : public pstd::noncopyable {
 public:
  class Worker {
   public:
    struct Arg {
      Arg(void* p, int i) : arg(p), idx(i) {}
      void* arg;
      int idx;
    };

    explicit Worker(ThreadPool* tp, int idx = 0) : start_(false), thread_pool_(tp), idx_(idx), arg_(tp, idx){};
    static void* WorkerMain(void* arg);

    int start();
    int stop();

   private:
    pthread_t thread_id_;
    std::atomic<bool> start_;
    ThreadPool* const thread_pool_;
    std::string worker_name_;
    int idx_;
    Arg arg_;
  };

  explicit ThreadPool(size_t worker_num, size_t max_queue_size, std::string thread_pool_name = "ThreadPool");
  virtual ~ThreadPool();

  int start_thread_pool();
  int stop_thread_pool();
  bool should_stop();
  void set_should_stop();

  void Schedule(TaskFunc func, void* arg);
  void DelaySchedule(uint64_t timeout, TaskFunc func, void* arg);
  size_t max_queue_size();
  size_t worker_size();
  void cur_queue_size(size_t* qsize);
  void cur_time_queue_size(size_t* qsize);
  std::string thread_pool_name();

 private:
  void runInThread(const int idx = 0);

 public:
  struct AdaptationContext {
    std::atomic<int32_t> value;

    explicit AdaptationContext() : value(0) {}
  };

 private:
  struct Node {
    Node* link_older = nullptr;
    Node* link_newer = nullptr;

    // true if task is TimeTask
    bool is_time_task;
    TimeTask task;

    Node(TaskFunc func, void* arg) : is_time_task(false), task(0, func, arg) {}
    Node(uint64_t exec_time, TaskFunc func, void* arg) : is_time_task(true), task(exec_time, func, arg) {}

    inline void Exec() { task.func(task.arg); }
    inline Node* Next() { return link_newer; }
  };

  // re-push some timer tasks which has been poped
  void ReDelaySchedule(Node* nodes);

  static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
    asm volatile("pause");
#elif defined(__aarch64__)
    asm volatile("wfe");
#elif defined(__powerpc64__)
    asm volatile("or 27,27,27");
#endif
    // it's okay for other platforms to be no-ops
  }

  Node* CreateMissingNewerLinks(Node* head, int* cnt);
  bool LinkOne(Node* node, std::atomic<Node*>* newest_node);

  uint16_t task_idx_;

  const uint8_t nworkers_per_link_ = 1;  // numer of workers per link
  const uint8_t nlinks_;                 // number of links (upper around)
  std::vector<std::atomic<Node*>> newest_node_;
  std::atomic<int> node_cnt_;  // for task
  std::vector<std::atomic<Node*>> time_newest_node_;
  std::atomic<int> time_node_cnt_;  // for time task

  const int queue_slow_size_;  // default value: min(worker_num_ * 10, max_queue_size_)
  size_t max_queue_size_;

  const uint64_t max_yield_usec_;
  const uint64_t slow_yield_usec_;

  AdaptationContext adp_ctx;

  const size_t worker_num_;
  std::string thread_pool_name_;
  std::vector<Worker*> workers_;
  std::atomic<bool> running_;
  std::atomic<bool> should_stop_;

  std::vector<pstd::Mutex> mu_;
  std::vector<pstd::CondVar> rsignal_;
};

}  // namespace net

#endif  // NET_INCLUDE_THREAD_POOL_H_
