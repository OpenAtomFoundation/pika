// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "include/throttle.h"
#include <gflags/gflags.h>
#include <algorithm>
#include "pstd/include/env.h"

DEFINE_uint64(raft_minimal_throttle_threshold_mb, 0, "minimal throttle throughput threshold per second");
namespace rsync {

Throttle::Throttle(size_t throttle_throughput_bytes, size_t check_cycle)
    : throttle_throughput_bytes_(throttle_throughput_bytes),
      last_throughput_check_time_us_(caculate_check_time_us_(pstd::NowMicros(), check_cycle)),
      cur_throughput_bytes_(0) {}

Throttle::~Throttle() {}

size_t Throttle::ThrottledByThroughput(size_t bytes) {
  std::unique_lock lock(keys_mutex_);
  size_t available_size = bytes;
  size_t now = pstd::NowMicros();
  size_t limit_throughput_bytes_s = std::max(static_cast<uint64_t>(throttle_throughput_bytes_),
                                             FLAGS_raft_minimal_throttle_threshold_mb * 1024 * 1024);
  size_t limit_per_cycle = limit_throughput_bytes_s / check_cycle_;
  if (cur_throughput_bytes_ + bytes > limit_per_cycle) {
    // reading another |bytes| excceds the limit
    if (now - last_throughput_check_time_us_ <= 1 * 1000 * 1000 / check_cycle_) {
      // if a time interval is less than or equal to a cycle, read more data
      // to make full use of the throughput of the current cycle.
      available_size = limit_per_cycle > cur_throughput_bytes_ ? limit_per_cycle - cur_throughput_bytes_ : 0;
      cur_throughput_bytes_ = limit_per_cycle;
    } else {
      // otherwise, read the data in the next cycle.
      available_size = bytes > limit_per_cycle ? limit_per_cycle : bytes;
      cur_throughput_bytes_ = available_size;
      last_throughput_check_time_us_ = caculate_check_time_us_(now, check_cycle_);
    }
  } else {
    // reading another |bytes| doesn't excced limit (less than or equal to),
    // put it in the current cycle
    available_size = bytes;
    cur_throughput_bytes_ += available_size;
  }
  keys_mutex_.unlock();
  return available_size;
}

void Throttle::ReturnUnusedThroughput(size_t acquired, size_t consumed, size_t elaspe_time_us) {
  size_t now = pstd::NowMicros();
  std::unique_lock lock(keys_mutex_);
  if (now - elaspe_time_us < last_throughput_check_time_us_) {
    // Tokens are aqured in last cycle, ignore
    return;
  }
  cur_throughput_bytes_ = std::max(cur_throughput_bytes_ - (acquired - consumed), size_t(0));
}
}  // namespace rsync
