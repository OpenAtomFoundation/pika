//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/options_helper.h"

#include <string>

namespace storage {

// strToInt may throw exception
static bool strToInt(const std::string& value, int32_t* num, int32_t base = 10) {
  size_t end;
  *num = std::stoi(value, &end, base);
  return end >= value.size();
}

// strToUint64 may throw exception
static bool strToUint64(const std::string& value, uint64_t* num, int32_t base = 10) {
  size_t end;
  *num = std::stoull(value, &end, base);
  return end >= value.size();
}

// strToUint32 may throw exception
static bool strToUint32(const std::string& value, uint32_t* num, int32_t base = 10) {
  uint64_t uint64Val;
  if (!strToUint64(value, &uint64Val)) {
    return false;
  }
  if ((uint64Val >> 32LL) == 0) {
    *num = static_cast<uint32_t>(uint64Val);
  } else {
    throw std::out_of_range(value);
  }
  return true;
}

bool ParseOptionMember(const MemberType& member_type, const std::string& value, char* member_address) {
  switch (member_type) {
    case MemberType::kInt: {
      int32_t intVal;
      if (!strToInt(value, &intVal)) {
        return false;
      }
      *reinterpret_cast<int32_t*>(member_address) = intVal;
      break;
    }
    case MemberType::kUint: {
      uint32_t uint32Val;
      if (!strToUint32(value, &uint32Val)) {
        return false;
      }
      *reinterpret_cast<uint32_t*>(member_address) = static_cast<uint32_t>(uint32Val);
      break;
    }
    case MemberType::kUint64T: {
      uint64_t uint64Val;
      if (!strToUint64(value, &uint64Val)) {
        return false;
      }
      *reinterpret_cast<uint64_t*>(member_address) = uint64Val;
      break;
    }
    case MemberType::kSizeT: {
      uint64_t uint64Val;
      if (!strToUint64(value, &uint64Val)) {
        return false;
      }
      *reinterpret_cast<size_t*>(member_address) = static_cast<size_t>(uint64Val);
      break;
    }
    default: {
      return false;
    }
  }
  return true;
}

}  // namespace storage
