//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_VALUE_FORMAT_H_
#define SRC_STRINGS_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"
#include "storage/storage_define.h"

namespace storage {
/*
* | type | value | reserve | cdate | timestamp |
* |  1B  |       |   16B   |   8B  |     8B    |
*/
class StringsValue : public InternalValue {
 public:
  explicit StringsValue(const rocksdb::Slice& user_value) : InternalValue(RedisType::kString, user_value) {}
  virtual rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + 2 * kTimestampLength + kTypeLength;
    char* dst = ReAllocIfNeeded(needed);
    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    EncodeFixed64(dst, ctime_);
    dst += kTimestampLength;
    EncodeFixed64(dst, etime_);
    return {start_, needed};
  }
};

class ParsedStringsValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStringsValue(std::string* internal_value_str) : ParsedInternalValue(internal_value_str) {
    // TODO Why need this logic; Mixficsol
    if (internal_value_str->size() >= kStringsValueMinLength) {
      size_t offset = 0;
      type_ = static_cast<RedisType>(static_cast<uint8_t>((*internal_value_str)[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_str->data() + offset,
                                   internal_value_str->size() - kStringsValueSuffixLength - offset);
      offset += user_value_.size();
      memcpy(reserve_, internal_value_str->data() + offset, kSuffixReserveLength);
      offset += kSuffixReserveLength;
      ctime_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(ctime_);
      etime_ = DecodeFixed64(internal_value_str->data() + offset);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStringsValue(const rocksdb::Slice& internal_value_slice) : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kStringsValueMinLength) {
      size_t offset = 0;
      type_ = static_cast<RedisType>(static_cast<uint8_t>(internal_value_slice[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_slice.data() + offset, internal_value_slice.size() - kStringsValueSuffixLength - offset);
      offset += user_value_.size();
      memcpy(reserve_, internal_value_slice.data() + offset, kSuffixReserveLength);
      offset += kSuffixReserveLength;
      ctime_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kTimestampLength;
      etime_ = DecodeFixed64(internal_value_slice.data() + offset);
    }
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(0, kTypeLength);
      value_->erase(value_->size() - kStringsValueSuffixLength, kStringsValueSuffixLength);
    }
  }

  // Strings type do not have version field;
  void SetVersionToValue() override {}

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
                  kStringsValueSuffixLength + kSuffixReserveLength;
      EncodeFixed64(dst, ctime_);
    }
  }

  void SetEtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
                  kStringsValueSuffixLength + kSuffixReserveLength + kTimestampLength;
      EncodeFixed64(dst, etime_);
    }
  }

private:
 // TODO Why need kStringsValueSuffixLength; Mixficsol
 const static size_t kStringsValueSuffixLength = 2 * kTimestampLength + kSuffixReserveLength;
 const static size_t kStringsValueMinLength = kStringsValueSuffixLength + kTypeLength;
};

}  //  namespace storage
#endif  // SRC_STRINGS_VALUE_FORMAT_H_
