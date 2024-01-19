// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_MONOTONIC_TIME_H
#define PIKA_MONOTONIC_TIME_H

#include <cstdint>

/* A counter in micro-seconds.  The 'monotime' type is provided for variables
 * holding a monotonic time.  This will help distinguish & document that the
 * variable is associated with the monotonic clock and should not be confused
 * with other types of time.*/
using monotime = uint64_t;

// Get monotonic time in microseconds
monotime getMonotonicUs();

#endif  // PIKA_MONOTONIC_TIME_H