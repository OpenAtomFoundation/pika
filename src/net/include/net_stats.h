// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef NET_INCLUDE_REDIS_STSTS_H_
#define NET_INCLUDE_REDIS_STSTS_H_

#include <atomic>

namespace net {

class NetworkStatistic {
 public:
  NetworkStatistic() = default;
  ~NetworkStatistic() = default;

  size_t NetInputBytes();
  size_t NetOutputBytes();
  size_t NetReplInputBytes();
  size_t NetReplOutputBytes();
  void IncrRedisInputBytes(uint64_t bytes);
  void IncrRedisOutputBytes(uint64_t bytes);
  void IncrReplInputBytes(uint64_t bytes);
  void IncrReplOutputBytes(uint64_t bytes);

 private:
  std::atomic<size_t> stat_net_input_bytes {0}; /* Bytes read from network. */
  std::atomic<size_t> stat_net_output_bytes {0}; /* Bytes written to network. */
  std::atomic<size_t> stat_net_repl_input_bytes {0}; /* Bytes read during replication, added to stat_net_input_bytes in 'info'. */
  std::atomic<size_t> stat_net_repl_output_bytes {0}; /* Bytes written during replication, added to stat_net_output_bytes in 'info'. */
};

}

#endif  // NET_INCLUDE_REDIS_STSTS_H_
