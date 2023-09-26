// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/pika_client_processor.h"

#include <glog/logging.h>

PikaClientProcessor::PikaClientProcessor(size_t worker_num, size_t max_queue_size, const std::string& name_prefix) {
  pool_ = std::make_unique<net::ThreadPool>(worker_num, max_queue_size, name_prefix + "Pool");
  for (size_t i = 0; i < worker_num; ++i) {
    bg_threads_.push_back(std::make_unique<net::BGThread>(max_queue_size));
    bg_threads_.back()->set_thread_name(name_prefix + "BgThread");
  }
}

PikaClientProcessor::~PikaClientProcessor() {
  LOG(INFO) << "PikaClientProcessor exit!!!";
}

int PikaClientProcessor::Start() {
  int res = pool_->start_thread_pool();
  if (res != net::kSuccess) {
    return res;
  }
  for (auto & bg_thread : bg_threads_) {
    res = bg_thread->StartThread();
    if (res != net::kSuccess) {
      return res;
    }
  }
  return res;
}

void PikaClientProcessor::Stop() {
  pool_->stop_thread_pool();
  for (auto & bg_thread : bg_threads_) {
    bg_thread->StopThread();
  }
}

void PikaClientProcessor::SchedulePool(net::TaskFunc func, void* arg) { pool_->Schedule(func, arg); }

void PikaClientProcessor::ScheduleBgThreads(net::TaskFunc func, void* arg, const std::string& hash_str) {
  std::size_t index = std::hash<std::string>{}(hash_str) % bg_threads_.size();
  bg_threads_[index]->Schedule(func, arg);
}

size_t PikaClientProcessor::ThreadPoolCurQueueSize() {
  size_t cur_size = 0;
  if (pool_) {
    pool_->cur_queue_size(&cur_size);
  }
  return cur_size;
}

size_t PikaClientProcessor::ThreadPoolMaxQueueSize() {
  size_t cur_size = 0;
  if (pool_) {
    cur_size = pool_->max_queue_size();
  }
  return cur_size;
}
