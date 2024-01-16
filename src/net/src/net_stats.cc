// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "net/include/net_stats.h"
#include <atomic>
#include <memory>

std::unique_ptr<net::NetworkStatistic> g_network_statistic;

namespace net {

size_t NetworkStatistic::NetInputBytes() { return stat_net_input_bytes.load(std::memory_order_relaxed); }

size_t NetworkStatistic::NetOutputBytes() { return stat_net_output_bytes.load(std::memory_order_relaxed); }

size_t NetworkStatistic::NetReplInputBytes() { return stat_net_repl_input_bytes.load(std::memory_order_relaxed); }

size_t NetworkStatistic::NetReplOutputBytes() { return stat_net_repl_output_bytes.load(std::memory_order_relaxed); }

void NetworkStatistic::IncrRedisInputBytes(uint64_t bytes) {
  stat_net_input_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void NetworkStatistic::IncrRedisOutputBytes(uint64_t bytes) {
  stat_net_output_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void NetworkStatistic::IncrReplInputBytes(uint64_t bytes) {
  stat_net_repl_input_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

void NetworkStatistic::IncrReplOutputBytes(uint64_t bytes) {
  stat_net_repl_output_bytes.fetch_add(bytes, std::memory_order_relaxed);
}

}  // namespace net