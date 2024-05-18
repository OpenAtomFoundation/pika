// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/thread_pool.h"
#include "net/src/net_thread_name.h"

#include <sys/time.h>

#include <cassert>
#include <thread>
#include <utility>

namespace net {

void* ThreadPool::Worker::WorkerMain(void* arg) {
  auto tp = static_cast<ThreadPool*>(arg);
  tp->runInThread();
  return nullptr;
}

int ThreadPool::Worker::start() {
  if (!start_.load()) {
    if (pthread_create(&thread_id_, nullptr, &WorkerMain, thread_pool_) != 0) {
      return -1;
    } else {
      start_.store(true);
      SetThreadName(thread_id_, thread_pool_->thread_pool_name() + "Worker");
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
    : newest_node_(nullptr),
      node_cnt_(0),
      queue_slow_size_(std::max(worker_num_ * 100, max_queue_size_)),
      max_yield_usec_(100),
      slow_yield_usec_(3),
      adp_ctx(),
      worker_num_(worker_num),
      max_queue_size_(max_queue_size),
      thread_pool_name_(std::move(thread_pool_name)),
      running_(false),
      should_stop_(false) {}

ThreadPool::~ThreadPool() { stop_thread_pool(); }

int ThreadPool::start_thread_pool() {
  if (!running_.load()) {
    should_stop_.store(false);
    for (size_t i = 0; i < worker_num_; ++i) {
      workers_.push_back(new Worker(this));
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
    rsignal_.notify_all();
    // wsignal_.notify_all();
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
  // stop until the size of tasks queue is not greater than max_queue_size_
  while (node_cnt_.load(std::memory_order_relaxed) >= max_queue_size_) {
    std::this_thread::yield();
  }
  // slow like above
  if (node_cnt_.load(std::memory_order_relaxed) >= queue_slow_size_) {
    std::this_thread::yield();
  }
  if (LIKELY(!should_stop())) {
    auto node = new Node(func, arg);
    LinkOne(node, &newest_node_);
    node_cnt_++;
    rsignal_.notify_one();
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
    auto node = new Node(exec_time, func, arg);
    LinkOne(node, &time_newest_node_);
    time_node_cnt_++;
    rsignal_.notify_all();
  }
}

size_t ThreadPool::max_queue_size() { return max_queue_size_; }

void ThreadPool::cur_queue_size(size_t* qsize) { *qsize = node_cnt_.load(std::memory_order_relaxed); }

void ThreadPool::cur_time_queue_size(size_t* qsize) { *qsize = time_node_cnt_.load(std::memory_order_relaxed); }

std::string ThreadPool::thread_pool_name() { return thread_pool_name_; }

void ThreadPool::runInThread() {
  Node* tmp = nullptr;
  Node* last = nullptr;
  Node* time_last = nullptr;
  while (LIKELY(!should_stop())) {
    std::unique_lock lock(mu_);
    rsignal_.wait(lock, [this]() {
      return newest_node_.load(std::memory_order_relaxed) != nullptr ||
             UNLIKELY(time_newest_node_.load(std::memory_order_relaxed) != nullptr) || UNLIKELY(should_stop());
    });
    lock.unlock();

  retry:
    if (UNLIKELY(should_stop())) {
      break;
    }

    last = newest_node_.exchange(nullptr);
    time_last = time_newest_node_.exchange(nullptr);
    if (last == nullptr && LIKELY(time_last == nullptr)) {
      // 1. loop for short time
      for (uint32_t tries = 0; tries < 200; ++tries) {
        if (newest_node_.load(std::memory_order_acquire) != nullptr) {
          last = newest_node_.exchange(nullptr);
          if (last != nullptr) {
            goto exec;
          }
        }
        if (UNLIKELY(time_newest_node_.load(std::memory_order_acquire) != nullptr)) {
          time_last = time_newest_node_.exchange(nullptr);
          if (time_last != nullptr) {
            goto exec;
          }
        }
        AsmVolatilePause();
      }

      // 2. loop for a little short time again
      const size_t kMaxSlowYieldsWhileSpinning = 3;
      auto& yield_credit = adp_ctx.value;
      bool update_ctx = false;
      bool would_spin_again = false;
      const int sampling_base = 256;

      update_ctx = Random::GetTLSInstance()->OneIn(sampling_base);

      if (update_ctx || yield_credit.load(std::memory_order_relaxed) >= 0) {
        auto spin_begin = std::chrono::steady_clock::now();

        size_t slow_yield_count = 0;

        auto iter_begin = spin_begin;
        while ((iter_begin - spin_begin) <= std::chrono::microseconds(max_yield_usec_)) {
          std::this_thread::yield();

          if (newest_node_.load(std::memory_order_acquire) != nullptr) {
            last = newest_node_.exchange(nullptr);
            if (last != nullptr) {
              would_spin_again = true;
              // success
              break;
            }
          }
          if (UNLIKELY(time_newest_node_.load(std::memory_order_acquire) != nullptr)) {
            time_last = time_newest_node_.exchange(nullptr);
            if (time_last != nullptr) {
              would_spin_again = true;
              // success
              break;
            }
          }

          auto now = std::chrono::steady_clock::now();
          if (now == iter_begin || now - iter_begin >= std::chrono::microseconds(slow_yield_usec_)) {
            ++slow_yield_count;
            if (slow_yield_count >= kMaxSlowYieldsWhileSpinning) {
              update_ctx = true;
              break;
            }
          }
          iter_begin = now;
        }
      }

      // update percentage of next loop 2
      if (update_ctx) {
        auto v = yield_credit.load(std::memory_order_relaxed);
        v = v - (v / 1024) + (would_spin_again ? 1 : -1) * 131072;
        yield_credit.store(v, std::memory_order_relaxed);
      }

      if (!would_spin_again) {
        // 3. wait for new task
        continue;
      }
    }

  exec:
    // do all normal tasks older than this task pointed last
    if (LIKELY(last != nullptr)) {
      int cnt = 0;
      auto first = CreateMissingNewerLinks(last, &cnt);
      assert(!first->is_time_task);
      node_cnt_ -= cnt;
      do {
        first->Exec();
        // node_cnt_--;
        tmp = first;
        first = first->Next();
        delete tmp;
      } while (first != nullptr);
    }

    // do all time tasks older than this task pointed time_last
    if (UNLIKELY(time_last != nullptr)) {
      int time_cnt = 0;
      auto time_first = CreateMissingNewerLinks(time_last, &time_cnt);
      // time_node_cnt_ -= time_cnt;
      do {
        // time task may block normal task
        auto now = std::chrono::system_clock::now();
        uint64_t unow = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();

        auto [exec_time, func, arg] = time_first->task;
        assert(time_first->is_time_task);
        if (unow >= exec_time) {
          time_first->Exec();
          time_node_cnt_--;
        } else {
          lock.lock();
          rsignal_.wait_for(lock, std::chrono::microseconds(exec_time - unow));
          time_first->Exec();
          time_node_cnt_--;
        }
        tmp = time_first;
        time_first = time_first->Next();
        delete tmp;
      } while (time_first != nullptr);
    }
    goto retry;
  }
}

ThreadPool::Node* ThreadPool::CreateMissingNewerLinks(Node* head, int* cnt) {
  assert(head != nullptr);
  Node* next = nullptr;
  *cnt = 1;
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
