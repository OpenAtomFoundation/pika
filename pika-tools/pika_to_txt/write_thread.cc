//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "iostream"
#include "fstream"
#include "string"
#include "write_thread.h"

#define MAX_QUEUE_SIZE 1024

WriteThread::WriteThread(const std::string& file_name)
    : should_stop_(false),
      file_name_(file_name),
      rsignal_(&data_queue_mutex_),
      wsignal_(&data_queue_mutex_) {}

void WriteThread::Load(const std::string& data) {
  data_queue_mutex_.Lock();
  if (data_queue_.size() < MAX_QUEUE_SIZE) {
    data_queue_.push(data);
    rsignal_.Signal();
    data_queue_mutex_.Unlock();
  } else {
    while (data_queue_.size() >= MAX_QUEUE_SIZE) {
      wsignal_.Wait();
    }
    data_queue_.push(data);
    rsignal_.Signal();
    data_queue_mutex_.Unlock();
  }
}

void WriteThread::Stop() {
  data_queue_mutex_.Lock();
  should_stop_ = true;
  rsignal_.Signal();
  data_queue_mutex_.Unlock();
}

void* WriteThread::ThreadMain() {
  std::fstream s(file_name_, s.binary | s.out);
  if (!s.is_open()) {
    std::cout << "failed to open " << file_name_ << ", exit..." << std::endl;
    exit(-1);
  } else {
    while (!should_stop_ || !data_queue_.empty()) {
      data_queue_mutex_.Lock();
      while (data_queue_.empty() && !should_stop_) {
        rsignal_.Wait();
      }
      data_queue_mutex_.Unlock();

      if (data_queue_.size() > 0) {
        data_queue_mutex_.Lock();
        std::string data = data_queue_.front();
        data_queue_.pop();
        wsignal_.Signal();
        data_queue_mutex_.Unlock();
        s.write(data.c_str(), data.size());
      }
    }
  }
  s.close();
  return NULL;
}
