// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/thread_pool.h"
#include "net/src/net_thread_name.h"
#include "pstd/include/env.h"

#include <sys/time.h>

#include <string>
#include <cassert>
#include <thread>
#include <utility>

namespace net {

void* ThreadPool::Worker::WorkerMain(void* p) {
  auto arg = static_cast<Arg*>(p);
  auto tp = static_cast<ThreadPool*>(arg->arg);
  tp->runInThread(arg->idx);
  return nullptr;
}

int ThreadPool::Worker::start() {
  if (!start_.load()) {
    if (pthread_create(&thread_id_, nullptr, &WorkerMain, &arg_) != 0) {
      return -1;
    } else {
      start_.store(true);
      std::string thread_id_str = std::to_string(reinterpret_cast<unsigned long>(thread_id_));
      SetThreadName(thread_id_, thread_pool_->thread_pool_name() + "_Worker_" + thread_id_str);
    }
  }
  return 0;
}

int ThreadPool::Worker::stop() {
  if (start_.load()) {
    if (pthread_join(thread_id_, nullptr) != 0) {
      return -1;
    } else {
      start_.store(false);
    }
  }
  return 0;
}

ThreadPool::ThreadPool(size_t worker_num, size_t max_queue_size, std::string thread_pool_name)
    : nlinks_(nworkers_per_link_ > worker_num ? 1 : (worker_num + nworkers_per_link_ - 1) / nworkers_per_link_),
      // : nlinks_(worker_num),
      newest_node_(nlinks_),
      node_cnt_(0),
      time_newest_node_(nlinks_),
      time_node_cnt_(0),
      max_queue_size_(1000 * nlinks_),
      queue_slow_size_(worker_num > 10 ? (100 * nlinks_) : std::max(worker_num, 3UL)),
      // queue_slow_size_(std::max(10UL, std::min(worker_num * max_queue_size / 100, max_queue_size))),
      max_yield_usec_(100),
      slow_yield_usec_(3),
      adp_ctx(),
      worker_num_(worker_num),
      thread_pool_name_(std::move(thread_pool_name)),
      running_(false),
      should_stop_(false),
      mu_(nlinks_),
      rsignal_(nlinks_) {
  for (size_t i = 0; i < nlinks_; ++i) {
    newest_node_[i] = nullptr;
    time_newest_node_[i] = nullptr;
  }
}

ThreadPool::~ThreadPool() { stop_thread_pool(); }

int ThreadPool::start_thread_pool() {
  if (!running_.load()) {
    should_stop_.store(false);
    for (size_t i = 0; i < worker_num_; ++i) {
      workers_.push_back(new Worker(this, i));
      int res = workers_[i]->start();
      if (res != 0) {
        return kCreateThreadError;
      }
    }
    running_.store(true);
  }
  return kSuccess;
}

int ThreadPool::stop_thread_pool() {
  int res = 0;
  if (running_.load()) {
    should_stop_.store(true);
    for (auto& r : rsignal_) {
      r.notify_all();
    }
    for (const auto worker : workers_) {
      res = worker->stop();
      if (res != 0) {
        break;
      } else {
        delete worker;
      }
    }
    workers_.clear();
    running_.store(false);
  }
  return res;
}

bool ThreadPool::should_stop() { return should_stop_.load(); }

void ThreadPool::set_should_stop() { should_stop_.store(true); }

void ThreadPool::Schedule(TaskFunc func, void* arg) {
  node_cnt_++;
  // stop until the size of tasks queue is not greater than max_queue_size_
  while (node_cnt_.load(std::memory_order_relaxed) >= max_queue_size_) {
    std::this_thread::yield();
    // pstd::SleepForMicroseconds(1);
  }
  // slow like above
  if (node_cnt_.load(std::memory_order_relaxed) >= queue_slow_size_) {
    std::this_thread::yield();
  }

  if (LIKELY(!should_stop())) {
    auto node = new Node(func, arg);
    auto idx = ++task_idx_;
    LinkOne(node, &newest_node_[idx % nlinks_]);
    rsignal_[idx % nlinks_].notify_one();
  }
}

/*
 * timeout is in millisecond
 */
void ThreadPool::DelaySchedule(uint64_t timeout, TaskFunc func, void* arg) {
  auto now = std::chrono::system_clock::now();
  uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  uint64_t exec_time = unow + timeout * 1000;

  if (LIKELY(!should_stop())) {
    auto idx = ++task_idx_;
    auto node = new Node(exec_time, func, arg);
    LinkOne(node, &newest_node_[idx % nlinks_]);
    time_node_cnt_++;
    rsignal_[idx % nlinks_].notify_all();
  }
}

size_t ThreadPool::max_queue_size() { return max_queue_size_; }

size_t ThreadPool::worker_size() { return worker_num_; }

void ThreadPool::cur_queue_size(size_t* qsize) { *qsize = node_cnt_.load(std::memory_order_relaxed); }

void ThreadPool::cur_time_queue_size(size_t* qsize) { *qsize = time_node_cnt_.load(std::memory_order_relaxed); }

std::string ThreadPool::thread_pool_name() { return thread_pool_name_; }

void ThreadPool::runInThread(const int idx) {
  Node* tmp = nullptr;
  Node* last = nullptr;
  Node* time_last = nullptr;

  auto& newest_node = newest_node_[idx % nlinks_];
  auto& mu = mu_[idx % nlinks_];
  auto& rsignal = rsignal_[idx % nlinks_];

  while (LIKELY(!should_stop())) {
    std::unique_lock lock(mu);
    rsignal.wait(lock, [this, &newest_node]() {
      return newest_node.load(std::memory_order_relaxed) != nullptr || UNLIKELY(should_stop());
    });
    lock.unlock();

    if (UNLIKELY(should_stop())) {
      break;
    }

  retry:
    last = newest_node.exchange(nullptr);
    if (last == nullptr) {
      continue;
    }

    // do all normal tasks older than this task pointed last
    int cnt = 1;
    auto first = CreateMissingNewerLinks(last, &cnt);
    assert(!first->is_time_task);
    do {
      first->Exec();
      tmp = first;
      first = first->Next();
      node_cnt_--;
      delete tmp;
    } while (first != nullptr);
    goto retry;
  }
}

void ThreadPool::ReDelaySchedule(Node* nodes) {
  while (LIKELY(!should_stop()) && nodes != nullptr) {
    auto idx = ++task_idx_;
    auto nxt = nodes->Next();
    nodes->link_newer = nullptr;
    // auto node = new Node(exec_time, func, arg);
    LinkOne(nodes, &newest_node_[idx % nlinks_]);
    time_node_cnt_++;
    rsignal_[idx % nlinks_].notify_all();
    nodes = nxt;
  }
}

ThreadPool::Node* ThreadPool::CreateMissingNewerLinks(Node* head, int* cnt) {
  assert(head != nullptr);
  assert(cnt != nullptr && *cnt == 1);
  Node* next = nullptr;
  while (true) {
    next = head->link_older;
    if (next == nullptr) {
      return head;
    }
    ++(*cnt);
    next->link_newer = head;
    head = next;
  }
}

bool ThreadPool::LinkOne(Node* node, std::atomic<Node*>* newest_node) {
  assert(newest_node != nullptr);
  auto nodes = newest_node->load(std::memory_order_relaxed);
  while (true) {
    node->link_older = nodes;
    if (newest_node->compare_exchange_weak(nodes, node)) {
      return (nodes == nullptr);
    }
  }
}
}  // namespace net