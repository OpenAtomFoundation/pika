//  Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_DATA_VALUE_FORMAT_H_
#define SRC_BASE_DATA_VALUE_FORMAT_H_

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "src/coding.h"
#include "src/mutex.h"
#include "src/base_value_format.h"
#include "storage/storage_define.h"

namespace storage {
/*
* hash/set/zset/list data value format
* | value | reserve | ctime |
* |       |   16B   |   8B  |
*/
class BaseDataValue : public InternalValue {
public:
 /*
  * The header of the Value field is initially initialized to knulltype
  */
  explicit BaseDataValue(const rocksdb::Slice& user_value) : InternalValue(RedisType::kNone, user_value) {}
  virtual ~BaseDataValue() {}

  virtual rocksdb::Slice Encode() {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + kTimestampLength;
    char* dst = ReAllocIfNeeded(needed);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), user_value_.size());
    dst += user_value_.size();
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    EncodeFixed64(dst, ctime_);
    dst += kTimestampLength;
    return rocksdb::Slice(start_pos, needed);
  }

private:
  const size_t kDefaultValueSuffixLength = kSuffixReserveLength + kTimestampLength;
};

class ParsedBaseDataValue : public ParsedInternalValue {
public:
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedBaseDataValue(std::string* value) : ParsedInternalValue(value) {
    if (value_->size() >= kBaseDataValueSuffixLength) {
      user_value_ = rocksdb::Slice(value_->data(), value_->size() - kBaseDataValueSuffixLength);
      memcpy(reserve_, value_->data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(value_->data() + user_value_.size() + kSuffixReserveLength);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedBaseDataValue(const rocksdb::Slice& value) : ParsedInternalValue(value)  {
    if (value.size() >= kBaseDataValueSuffixLength) {
      user_value_ = rocksdb::Slice(value.data(), value.size() - kBaseDataValueSuffixLength);
      memcpy(reserve_, value.data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(value.data() + user_value_.size() + kSuffixReserveLength);
    }
  }

  virtual ~ParsedBaseDataValue() = default;

  void SetEtimeToValue() override {}

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kTimestampLength;
      EncodeFixed64(dst, ctime_);
    }
  }

  void SetReserveToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kBaseDataValueSuffixLength;
      memcpy(dst, reserve_, kSuffixReserveLength);
    }
  }

  virtual void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kBaseDataValueSuffixLength, kBaseDataValueSuffixLength);
    }
  }

protected:
  virtual void SetVersionToValue() override {};

private:
  const size_t kBaseDataValueSuffixLength = kSuffixReserveLength + kTimestampLength;
};

}  //  namespace storage
#endif  // SRC_BASE_VALUE_FORMAT_H_
