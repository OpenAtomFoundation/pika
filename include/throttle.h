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
  int64_t GetWaitTimeoutMs(){
      return wait_timout_ms_.load();
  }
  void ResetThrottleThroughputBytes(size_t new_throughput_bytes_per_s){
        if(last_rsync_config_updated_time_ms_.load() + 1000 < pstd::NowMilliSeconds()){
            //maximum update frequency of rsync config:1 time per sec
            {
                std::lock_guard guard(keys_mutex_);
                throttle_throughput_bytes_.store(new_throughput_bytes_per_s);
            }
            LOG(INFO) << "The conf item [throttle-bytes-per-second] is changed by Config Set command. "
                         "The rsync rate limit now is "
                      << new_throughput_bytes_per_s << "(Which Is Around " << (new_throughput_bytes_per_s >> 20) << " MB/s)";
            last_rsync_config_updated_time_ms_.store(pstd::NowMilliSeconds());
        }
    };
    void ResetRsyncTimeout(int64_t new_timeout_ms) {
        if (last_rsync_config_updated_time_ms_.load() + 1000 < pstd::NowMilliSeconds()) {
            wait_timout_ms_.store(new_timeout_ms);
            // maximum update frequency of rsync config:1 time per sec
            LOG(INFO) << "The conf item [rsync-timeout-ms] is changed by Config Set command. "
                         "The rsync-timeout-ms now is " << new_timeout_ms << " ms";
            last_rsync_config_updated_time_ms_.store(pstd::NowMilliSeconds());
        }
    }

  size_t ThrottledByThroughput(size_t bytes);

  void ReturnUnusedThroughput(size_t acquired, size_t consumed, size_t elaspe_time_us);
  static Throttle& GetInstance() {
    static Throttle instance(g_pika_conf->throttle_bytes_per_second(), 10);
    return instance;
  }
private:
  // Speed rate defined by user, it's protected by keys_mutex_
  std::atomic<size_t> throttle_throughput_bytes_ = 100 * 1024 * 1024;
  // The num of tasks doing install_snapshot
  std::atomic<size_t> last_throughput_check_time_us_;
  std::atomic<size_t> cur_throughput_bytes_;
  // User defined check cycles of throughput per second
  size_t check_cycle_ = 10;
  pstd::Mutex keys_mutex_;
  size_t caculate_check_time_us_(int64_t current_time_us, int64_t check_cycle) {
    size_t base_aligning_time_us = 1000 * 1000 / check_cycle;
    return current_time_us / base_aligning_time_us * base_aligning_time_us;
  }
  // Rsync timeout value used by WaitObject,defined by user, It's NOT protected by keys_mutex_
  std::atomic<int64_t> wait_timout_ms_{1000};
  //1 when multi thread changing rsync rate and timeout at the same time,make them take effect sequentially
  //2 maximum update frequency of rsync config: 1 time per sec
  std::atomic<uint64_t> last_rsync_config_updated_time_ms_{0};
};
}  // end namespace rsync
#endif

