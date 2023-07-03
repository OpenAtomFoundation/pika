//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_VALUE_FORMAT_H_
#define SRC_STRINGS_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace storage {

class StringsValue : public InternalValue {
 public:
  explicit StringsValue(const rocksdb::Slice& user_value)
      : InternalValue(user_value) {}
  size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed32(dst, timestamp_);
    return usize + sizeof(int32_t);
  }
};

class ParsedStringsValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedStringsValue(std::string* internal_value_str)
      : ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kStringsValueSuffixLength) {
      user_value_ = rocksdb::Slice(
          internal_value_str->data(),
          internal_value_str->size() - kStringsValueSuffixLength);
      timestamp_ =
          DecodeFixed32(internal_value_str->data() +
                        internal_value_str->size() - kStringsValueSuffixLength);
    }
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedStringsValue(const rocksdb::Slice& internal_value_slice)
      : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kStringsValueSuffixLength) {
      user_value_ = rocksdb::Slice(
          internal_value_slice.data(),
          internal_value_slice.size() - kStringsValueSuffixLength);
      timestamp_ = DecodeFixed32(internal_value_slice.data() +
                                 internal_value_slice.size() -
                                 kStringsValueSuffixLength);
    }
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kStringsValueSuffixLength,
                    kStringsValueSuffixLength);
    }
  }

  // Strings type do not have version field;
  void SetVersionToValue() override {}

  void SetTimestampToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() -
                  kStringsValueSuffixLength;
      EncodeFixed32(dst, timestamp_);
    }
  }

  rocksdb::Slice value() { return user_value_; }

  static const size_t kStringsValueSuffixLength = sizeof(int32_t);
};

}  //  namespace storage
#endif  // SRC_STRINGS_VALUE_FORMAT_H_
