//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <memory.h>
#include <cassert>
#include <cstdint>
#include <string>
#include <vector>
#include "src/coding.h"

namespace storage {

#define kINVALID_TREE_ID 0

using streamID = struct streamID {
  streamID(uint64_t _ms, uint64_t _seq) : ms(_ms), seq(_seq) {}
  bool operator==(const streamID& other) const { return ms == other.ms && seq == other.seq; }
  bool operator<(const streamID& other) const { return ms < other.ms || (ms == other.ms && seq < other.seq); }
  bool operator>(const streamID& other) const { return ms > other.ms || (ms == other.ms && seq > other.seq); }
  bool operator<=(const streamID& other) const { return ms < other.ms || (ms == other.ms && seq <= other.seq); }
  bool operator>=(const streamID& other) const { return ms > other.ms || (ms == other.ms && seq >= other.seq); }
  std::string ToString() const { return std::to_string(ms) + "-" + std::to_string(seq); }

  // We must store the streamID in memory in big-endian format. This way, our comparison of the serialized streamID byte
  // code will be equivalent to the comparison of the uint64_t numbers.
  inline void EncodeUint64InBigEndian(char* buf, uint64_t value) const {
    if (kLittleEndian) {
      // little endian, reverse the bytes
      for (int i = 7; i >= 0; --i) {
        buf[i] = static_cast<char>(value & 0xff);
        value >>= 8;
      }
    } else {
      // big endian, just copy the bytes
      memcpy(buf, &value, sizeof(value));
    }
  }

  inline uint64_t DecodeUint64OfBigEndian(const char* ptr) {
    uint64_t value;
    if (kLittleEndian) {
      // little endian, reverse the bytes
      value = 0;
      for (int i = 0; i < 8; ++i) {
        value <<= 8;
        value |= static_cast<unsigned char>(ptr[i]);
      }
    } else {
      // big endian, just copy the bytes
      memcpy(&value, ptr, sizeof(value));
    }
    return value;
  }

  std::string Serialize() const {
    std::string dst;
    dst.resize(sizeof(ms) + sizeof(seq));
    EncodeUint64InBigEndian(&dst[0], ms);
    EncodeUint64InBigEndian(&dst[0] + sizeof(ms), seq);
    return dst;
  }

  void DeserializeFrom(std::string& src) {
    assert(src.size() == sizeof(ms) + sizeof(seq));
    ms = DecodeUint64OfBigEndian(&src[0]);
    seq = DecodeUint64OfBigEndian(&src[0] + sizeof(ms));
  }

  streamID() = default;
  uint64_t ms = 0;  /* Unix time in milliseconds. */
  uint64_t seq = 0; /* Sequence number. */
};

static const streamID kSTREAMID_MAX = streamID(UINT64_MAX, UINT64_MAX);
static const streamID kSTREAMID_MIN = streamID(0, 0);

enum StreamTrimStrategy { TRIM_STRATEGY_NONE, TRIM_STRATEGY_MAXLEN, TRIM_STRATEGY_MINID };

using tree_id_t = uint32_t;

using stream_ms_t = uint64_t;

}  // namespace storage
