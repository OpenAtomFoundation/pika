// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifdef __APPLE__  // Mac
#include <mach/mach_time.h>

#include "include/pika_monotonic_time.h"

monotime getMonotonicUs() {
  static mach_timebase_info_data_t timebase;
  if (timebase.denom == 0) {
    mach_timebase_info(&timebase);
  }
  uint64_t nanos = mach_absolute_time() * timebase.numer / timebase.denom;
  return nanos / 1000;
}

#elif __linux__  // Linux

#ifdef __x86_64__  // x86_64

#include <ctime>

#include "include/pika_monotonic_time.h"

monotime getMonotonicUs() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000 + static_cast<uint64_t>(ts.tv_nsec) / 1000;
}

#elif __arm__ || __aarch64__  // ARM

#include <sys/time.h>

#include "include/pika_monotonic_time.h"

uint64_t getMonotonicUs() {
  timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + static_cast<uint64_t>(tv.tv_usec);
}

#else
#error "Unsupported architecture for Linux"
#endif  // __x86_64__, __arm__

#else
#error "Unsupported platform"
#endif  // __APPLE__, __linux__
