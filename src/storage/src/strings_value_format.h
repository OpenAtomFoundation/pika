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
* | value | reserve | cdate | timestamp |
* |       |   16B   |   8B  |     8B    |
*/
class StringsValue : public InternalValue {
 public:
  explicit StringsValue(const rocksdb::Slice& user_value) : InternalValue(user_value) {}
  virtual rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kSuffixReserveLength + 2 * kTimestampLength;
    char* dst = ReAllocIfNeeded(needed);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    memcpy(dst, reserve_, kSuffixReserveLength);
    dst += kSuffixReserveLength;
    EncodeFixed64(dst, ctime_);
    dst += kTimestampLength;
    EncodeFixed64(dst, etime_);
    return rocksdb::Slice(start_pos, needed);
  }
};

class ParsedStringsValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStringsValue(std::string* internal_value_str) : ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kStringsValueSuffixLength) {
      user_value_ = rocksdb::Slice(internal_value_str->data(), internal_value_str->size() - kStringsValueSuffixLength);
      memcpy(reserve_, internal_value_str->data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(internal_value_str->data() + user_value_.size() + kSuffixReserveLength);
      etime_ = DecodeFixed64(internal_value_str->data() + user_value_.size() + kSuffixReserveLength + kTimestampLength);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStringsValue(const rocksdb::Slice& internal_value_slice) : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kStringsValueSuffixLength) {
      user_value_ = rocksdb::Slice(internal_value_slice.data(), internal_value_slice.size() - kStringsValueSuffixLength);
      memcpy(reserve_, internal_value_slice.data() + user_value_.size(), kSuffixReserveLength);
      ctime_ = DecodeFixed64(internal_value_slice.data() + user_value_.size() + kSuffixReserveLength);
      etime_ = DecodeFixed64(internal_value_slice.data() + user_value_.size() + kSuffixReserveLength + kTimestampLength);
    }
  }

  void StripSuffix() override {
    if (value_) {
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
  const size_t kStringsValueSuffixLength =  2 * kTimestampLength + kSuffixReserveLength;
};

}  //  namespace storage
#endif  // SRC_STRINGS_VALUE_FORMAT_H_
