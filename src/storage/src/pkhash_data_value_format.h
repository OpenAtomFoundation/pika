//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_PKHASH_DATA_VALUE_FORMAT_H_
#define SRC_PKHASH_DATA_VALUE_FORMAT_H_

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "base_value_format.h"
#include "src/coding.h"
#include "src/mutex.h"
#include "storage/storage_define.h"

namespace storage {
/*
 * pika expire hash data value format
 * use etime to store expire time
 * | value | reserve | ctime | etime |
 * |       |   16B   |   8B  |   8B  |
 */
class PKHashDataValue : public InternalValue {
 public:
  /*
   * The header of the Value field is initially initialized to knulltype
   */
  explicit PKHashDataValue(const rocksdb::Slice& user_value) : InternalValue(DataType::kNones, user_value) {}
  virtual ~PKHashDataValue() {}

  virtual rocksdb::Slice Encode() {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + kTimestampLength * 2;
    char* dst = ReAllocIfNeeded(needed);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), user_value_.size());
    dst += user_value_.size();
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    EncodeFixed64(dst, ctime_);
    dst += kTimestampLength;
    EncodeFixed64(dst, etime_);
    dst += kTimestampLength;  // todo(DDD) 待确认，看这个是否需要。

    return rocksdb::Slice(start_pos, needed);
  }

 private:
  const size_t kDefaultValueSuffixLength = kSuffixReserveLength + kTimestampLength * 2;
};

class ParsedPKHashDataValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedPKHashDataValue(std::string* value) : ParsedInternalValue(value) {
    if (value_->size() >= kPKHashDataValueSuffixLength) {
      user_value_ = rocksdb::Slice(value_->data(), value_->size() - kPKHashDataValueSuffixLength);
      memcpy(reserve_, value_->data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(value_->data() + user_value_.size() + kSuffixReserveLength);
      etime_ = DecodeFixed64(value_->data() + user_value_.size() + kSuffixReserveLength + kTimestampLength);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedPKHashDataValue(const rocksdb::Slice& value) : ParsedInternalValue(value) {
    if (value.size() >= kPKHashDataValueSuffixLength) {
      user_value_ = rocksdb::Slice(value.data(), value.size() - kPKHashDataValueSuffixLength);
      memcpy(reserve_, value.data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(value.data() + user_value_.size() + kSuffixReserveLength);
      etime_ = DecodeFixed64(value.data() + user_value_.size() + kSuffixReserveLength + kTimestampLength);
    }
  }

  virtual ~ParsedPKHashDataValue() = default;

  void SetEtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kTimestampLength;
      EncodeFixed64(dst, etime_);
    }
  }

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kTimestampLength - kTimestampLength;
      EncodeFixed64(dst, ctime_);
    }
  }

  void SetReserveToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kPKHashDataValueSuffixLength;
      memcpy(dst, reserve_, kSuffixReserveLength);
    }
  }

  virtual void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kPKHashDataValueSuffixLength, kPKHashDataValueSuffixLength);
    }
  }

  void SetTimestamp(int64_t timestamp) {
    etime_ = timestamp;
    SetEtimeToValue();
  }

 protected:
  virtual void SetVersionToValue() override {};

 private:
  const size_t kPKHashDataValueSuffixLength = kSuffixReserveLength + kTimestampLength * 2;
};

}  //  namespace storage
#endif  // SRC_BASE_VALUE_FORMAT_H_
