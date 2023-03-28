//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_REDIS_HYPERLOGLOG_H_
#define SRC_REDIS_HYPERLOGLOG_H_

#include <iostream>
#include <string>

namespace storage {

class HyperLogLog {
 public:
  HyperLogLog(uint8_t precision, std::string origin_resiter);
  ~HyperLogLog();

  double Estimate() const;
  double FirstEstimate() const;
  uint32_t CountZero() const;
  double Alpha() const;
  uint8_t Nctz(uint32_t x, int b);

  std::string Add(const char* str, uint32_t len);
  std::string Merge(const HyperLogLog& hll);

 protected:
  uint32_t m_;  // register bit width
  uint32_t b_;  // regieter size
  double alpha_;
  char* register_;  // register;
};

}  // namespace storage

#endif  // SRC_REDIS_HYPERLOGLOG_H_

