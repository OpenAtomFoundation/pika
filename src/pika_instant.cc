// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "../include/pika_instant.h"
#include <string>

/* Return the mean of all the samples. */
double Instant::getInstantaneousMetric(std::string metric) {
  size_t j;
  size_t sum = 0;

  for (j = 0; j < STATS_METRIC_SAMPLES; j++) sum += inst_metrics_[metric].samples[j];

  return sum / STATS_METRIC_SAMPLES;
}

/* ======================= Cron: called every 5 s ======================== */

/* Add a sample to the instantaneous metric. This function computes the quotient
 * of the increment of value and base, which is useful to record operation count
 * per second, or the average time consumption of an operation.
 *
 * current_value - The dividend
 * current_base - The divisor
 * */
void Instant::trackInstantaneousMetric(std::string metric, size_t current_value, size_t current_base, size_t factor) {
  if (inst_metrics_[metric].last_sample_base > 0) {
    size_t base = current_base - inst_metrics_[metric].last_sample_base;
    size_t value = current_value - inst_metrics_[metric].last_sample_value;
    size_t avg = base > 0 ? (value * factor / base) : 0;
    inst_metrics_[metric].samples[inst_metrics_[metric].idx] = avg;
    inst_metrics_[metric].idx++;
    inst_metrics_[metric].idx %= STATS_METRIC_SAMPLES;
  }
  inst_metrics_[metric].last_sample_base = current_base;
  inst_metrics_[metric].last_sample_value = current_value;
}