// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_DATA_DISTRIBUTION_H_
#define PIKA_DATA_DISTRIBUTION_H_

#include "pstd/include/pstd_status.h"

// polynomial reserved Crc32 magic num
const uint32_t IEEE_POLY = 0xedb88320;

class PikaDataDistribution {
 public:
 virtual ~PikaDataDistribution() = default; 
  // Initialization
  virtual void Init() = 0;
  // key map to partition id
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num) = 0;
};

class HashModulo : public PikaDataDistribution {
 public:
  virtual ~HashModulo() = default;
  virtual void Init();
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num);
};

class Crc32 : public PikaDataDistribution {
 public:
  virtual void Init();
  virtual uint32_t Distribute(const std::string& str, uint32_t partition_num);
 private:
  void Crc32TableInit(uint32_t poly);
  uint32_t Crc32Update(uint32_t crc, const char* buf, int len);
  uint32_t crc32tab[256];
};

std::string GetHashkey(const std::string& key);

#endif
