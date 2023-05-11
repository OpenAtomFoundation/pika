//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "write_thread.h"
#include "fstream"
#include "iostream"
#include "string"

#define MAX_QUEUE_SIZE 1024

WriteThread::WriteThread(const std::string& file_name) : should_stop_(false), file_name_(file_name) {}

void WriteThread::Load(const std::string& data) {
  std::unique_lock lock(data_queue_mutex_);

  if (data_queue_.size() < MAX_QUEUE_SIZE) {
    data_queue_.push(data);
    rsignal_.notify_one();
  } else {
    wsignal_.wait(lock, [this] { return data_queue_.size() < MAX_QUEUE_SIZE; });
    data_queue_.push(data);
    rsignal_.notify_one();
  }
}

void WriteThread::Stop() {
  std::unique_lock lock(data_queue_mutex_);
  should_stop_ = true;
  rsignal_.notify_one();
}

void* WriteThread::ThreadMain() {
  std::fstream s(file_name_, s.binary | s.out);
  if (!s.is_open()) {
    std::cout << "failed to open " << file_name_ << ", exit..." << std::endl;
    exit(-1);
  } else {
    while (!should_stop_ || !data_queue_.empty()) {
      {
        std::unique_lock lock(data_queue_mutex_);
        rsignal_.wait(lock, [this] { return !data_queue_.empty() || should_stop_; });
      }

      if (data_queue_.size() > 0) {
        data_queue_mutex_.lock();
        std::string data = data_queue_.front();
        data_queue_.pop();
        data_queue_mutex_.unlock();
        wsignal_.notify_one();
        s.write(data.c_str(), data.size());
      }
    }
  }
  s.close();
  return nullptr;
}
