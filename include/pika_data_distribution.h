// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DATA_DISTRIBUTION_H_
#define PIKA_DATA_DISTRIBUTION_H_

#include <cstdint>
#include <string>

// polynomial reserved Crc32 magic num
const uint32_t IEEE_POLY = 0xedb88320;

class PikaDataDistribution {
 public:
  virtual ~PikaDataDistribution() = default;
  // Initialization
  virtual void Init() = 0;
  virtual uint32_t Distribute(const std::string& str) = 0;
};

class HashModulo : public PikaDataDistribution {
 public:
  ~HashModulo() override = default;
  void Init() override;
  uint32_t Distribute(const std::string& str) override;
};

#endif
