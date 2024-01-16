// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_PIKA_INSTANT_H
#define PIKA_PIKA_INSTANT_H

#include <string>
#include <unordered_map>

inline constexpr size_t STATS_METRIC_SAMPLES = 16; /* Number of samples per metric. */
inline const std::string STATS_METRIC_NET_INPUT = "stats_metric_net_input";
inline const std::string STATS_METRIC_NET_OUTPUT = "stats_metric_net_output";
inline const std::string STATS_METRIC_NET_INPUT_REPLICATION = "stats_metric_net_input_replication";
inline const std::string STATS_METRIC_NET_OUTPUT_REPLICATION = "stats_metric_net_output_replication";

/* The following two are used to track instantaneous metrics, like
 * number of operations per second, network traffic. */
struct InstMetric {
  size_t last_sample_base;  /* The divisor of last sample window */
  size_t last_sample_value; /* The dividend of last sample window */
  double samples[STATS_METRIC_SAMPLES];
  int idx;
};

class Instant {
 public:
  Instant() = default;
  ~Instant() = default;

  void trackInstantaneousMetric(std::string metric, size_t current_value, size_t current_base, size_t factor);
  double getInstantaneousMetric(std::string metric);

 private:
  std::unordered_map<std::string, InstMetric> inst_metrics_;
};

#endif  // PIKA_PIKA_INSTANT_H
