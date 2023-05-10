//  Copyright (c) 2018-present The pika-tools Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_WRITE_THREAD_H_
#define INCLUDE_WRITE_THREAD_H_

#include <queue>

#include "net/include/net_thread.h"
#include "pstd/include/pstd_mutex.h"

class WriteThread : public net::Thread {
 public:
  WriteThread(const std::string& file_name);
  void Load(const std::string& data);
  void Stop();

 private:
  void* ThreadMain() override;
  bool should_stop_;
  std::string file_name_;
  pstd::CondVar rsignal_;
  pstd::CondVar wsignal_;
  pstd::Mutex data_queue_mutex_;
  std::queue<std::string> data_queue_;
};

#endif  // INCLUDE_WRITE_THREAD_H_
