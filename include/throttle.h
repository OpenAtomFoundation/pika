// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef THROTTLE_H_
#define THROTTLE_H_

#include <atomic>
#include "pstd/include/pstd_mutex.h"
#include "pika_conf.h"

extern std::unique_ptr<PikaConf> g_pika_conf;

namespace rsync {
class Throttle {
 public:
  Throttle() {}
  Throttle(size_t throttle_throughput_bytes, size_t check_cycle);
  ~Throttle();
  size_t ThrottledByThroughput(size_t bytes);
  void ReturnUnusedThroughput(size_t acquired, size_t consumed, size_t elaspe_time_us);
  static Throttle& GetInstance() {
    static Throttle instance(g_pika_conf->throttle_bytes_per_second(), 10);
    return instance;
  }
  void SetThrottleThroughputBytes(size_t new_throughput_bytes_per_s){
      throttle_throughput_bytes_.store(new_throughput_bytes_per_s);
  };
 private:
  std::atomic<int32_t>calculate_count{0};
  std::atomic<size_t> throttle_throughput_bytes_ = 100 * 1024 * 1024;
  // the num of tasks doing install_snapshot
  std::atomic<size_t> last_throughput_check_time_us_;
  std::atomic<size_t> cur_throughput_bytes_;
  // user defined check cycles of throughput per second
  size_t check_cycle_ = 10;
  pstd::Mutex keys_mutex_;
  size_t caculate_check_time_us_(int64_t current_time_us, int64_t check_cycle) {
    size_t base_aligning_time_us = 1000 * 1000 / check_cycle;
    return current_time_us / base_aligning_time_us * base_aligning_time_us;
  }
};
}  // end namespace rsync
#endif

