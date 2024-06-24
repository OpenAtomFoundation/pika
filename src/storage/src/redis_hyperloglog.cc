//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include <algorithm>
#include <cmath>
#include <string>
#include <cstring>

#include "src/storage_murmur3.h"
#include "storage/storage_define.h"
#include "src/redis.h"
#include "src/mutex.h"
#include "src/redis_hyperloglog.h"
#include "src/scope_record_lock.h"

namespace storage {

const int32_t HLL_HASH_SEED = 313;

HyperLogLog::HyperLogLog(uint8_t precision, std::string origin_register) {
  b_ = precision;
  m_ = 1 << precision;
  alpha_ = Alpha();
  register_ = std::make_unique<char[]>(m_);
  for (uint32_t i = 0; i < m_; ++i) {
    register_[i] = 0;
  }
  if (!origin_register.empty()) {
    for (uint32_t i = 0; i < m_; ++i) {
      register_[i] = origin_register[i];
    }
  }
}

HyperLogLog::~HyperLogLog() = default;

std::string HyperLogLog::Add(const char* value, uint32_t len) {
  uint32_t hash_value;
  MurmurHash3_x86_32(value, static_cast<int32_t>(len), HLL_HASH_SEED, static_cast<void*>(&hash_value));
  uint32_t index = hash_value & ((1 << b_) - 1);
  uint8_t rank = Nctz((hash_value >> b_), static_cast<int32_t>(32 - b_));
  if (rank > register_[index]) { register_[index] = static_cast<char>(rank);
}
  std::string result(m_, 0);
  for (uint32_t i = 0; i < m_; ++i) {
    result[i] = register_[i];
  }
  return result;
}

double HyperLogLog::Estimate() const {
  double estimate = FirstEstimate();
  if (estimate <= 2.5 * m_) {
    uint32_t zeros = CountZero();
    if (zeros != 0) {
      estimate = m_ * log(static_cast<double>(m_) / zeros);
    }
  } else if (estimate > pow(2, 32) / 30.0) {
    estimate = log1p(estimate * -1 / pow(2, 32)) * pow(2, 32) * -1;
  }
  return estimate;
}

double HyperLogLog::FirstEstimate() const {
  double estimate;
  double sum = 0.0;
  for (uint32_t i = 0; i < m_; i++) {
    sum += 1.0 / (1 << register_[i]);
  }

  estimate = alpha_ * m_ * m_ / sum;
  return estimate;
}

double HyperLogLog::Alpha() const {
  switch (m_) {
    case 16:
      return 0.673;
    case 32:
      return 0.697;
    case 64:
      return 0.709;
    default:
      return 0.7213 / (1 + 1.079 / m_);
  }
}

uint32_t HyperLogLog::CountZero() const {
  uint32_t count = 0;
  for (uint32_t i = 0; i < m_; i++) {
    if (register_[i] == 0) {
      count++;
    }
  }
  return count;
}

std::string HyperLogLog::Merge(const HyperLogLog& hll) {
  if (m_ != hll.m_) {
    // TODO(shq) the number of registers doesn't match
  }
  for (uint32_t r = 0; r < m_; r++) {
    if (register_[r] < hll.register_[r]) {
      register_[r] = static_cast<char>(register_[r] | hll.register_[r]);
    }
  }

  std::string result(m_, 0);
  for (uint32_t i = 0; i < m_; ++i) {
    result[i] = register_[i];
  }
  return result;
}

// ::__builtin_ctz(x): return the first number of '0' after the first '1' from the right
uint8_t HyperLogLog::Nctz(uint32_t x, int b) { return static_cast<uint8_t>(std::min(b, ::__builtin_ctz(x))) + 1; }


bool IsHyperloglogObj(const std::string* internal_value_str) {
    size_t kStringsValueSuffixLength = 2 * kTimestampLength + kSuffixReserveLength;
    char reserve[16] = {0};
    size_t offset = internal_value_str->size() - kStringsValueSuffixLength;
    memcpy(reserve, internal_value_str->data() + offset, kSuffixReserveLength);

    //if first bit in reserve is 0 , then this obj is string; else the obj is hyperloglog
    return (reserve[0] & hyperloglog_reserve_flag) != 0;;
}

Status Redis::HyperloglogGet(const Slice &key, std::string* value) {
    value->clear();

    BaseKey base_key(key);
    Status s = db_->Get(default_read_options_, base_key.Encode(), value);
    std::string meta_value = *value;
    if (!s.ok()) {
        return s;
    }
    if (!ExpectedMetaValue(DataType::kStrings, meta_value)) {
        if (ExpectedStale(meta_value)) {
            s = Status::NotFound();
        } else {
            return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                           ", expect type: " + "hyperloglog " + "get type: " +
                                           DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
        }
    } else if (!IsHyperloglogObj(value)) {
        return Status::InvalidArgument("WRONGTYPE, key: " + key.ToString() +
                                       ",expect type: " + "hyperloglog " + "get type: " +
                                       DataTypeStrings[static_cast<int>(GetMetaValueType(meta_value))]);
    } else {
        ParsedStringsValue parsed_strings_value(value);
        if (parsed_strings_value.IsStale()) {
            value->clear();
            return Status::NotFound("Stale");
        } else {
            parsed_strings_value.StripSuffix();
        }
    }
    return s;
}

Status Redis::HyperloglogSet(const Slice &key, const Slice &value) {
    HyperloglogValue hyperloglog_value(value);
    ScopeRecordLock l(lock_mgr_, key);

    BaseKey base_key(key);
    return db_->Put(default_write_options_, base_key.Encode(), hyperloglog_value.Encode());
}

}  // namespace storage