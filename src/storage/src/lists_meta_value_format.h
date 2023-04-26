//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_META_VALUE_FORMAT_H_
#define SRC_LISTS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"

namespace storage {

const uint64_t InitalLeftIndex = 9223372036854775807;
const uint64_t InitalRightIndex = 9223372036854775808U;

class ListsMetaValue : public InternalValue {
 public:
  explicit ListsMetaValue(const rocksdb::Slice& user_value)
      : InternalValue(user_value), left_index_(InitalLeftIndex), right_index_(InitalRightIndex) {}

  size_t AppendTimestampAndVersion() override {
    size_t usize = user_value_.size();
    char* dst = start_;
    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    EncodeFixed32(dst, timestamp_);
    return usize + 2 * sizeof(int32_t);
  }

  virtual size_t AppendIndex() {
    char* dst = start_;
    dst += user_value_.size() + 2 * sizeof(int32_t);
    EncodeFixed64(dst, left_index_);
    dst += sizeof(int64_t);
    EncodeFixed64(dst, right_index_);
    return 2 * sizeof(int64_t);
  }

  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2 + sizeof(int64_t) * 2;

  const rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion() + AppendIndex();
    return rocksdb::Slice(start_, len);
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    return version_;
  }

  uint64_t left_index() { return left_index_; }

  void ModifyLeftIndex(uint64_t index) { left_index_ -= index; }

  uint64_t right_index() { return right_index_; }

  void ModifyRightIndex(uint64_t index) { right_index_ += index; }

 private:
  uint64_t left_index_;
  uint64_t right_index_;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedListsMetaValue(std::string* internal_value_str)
      : ParsedInternalValue(internal_value_str), count_(0), left_index_(0), right_index_(0) {
    assert(internal_value_str->size() >= kListsMetaValueSuffixLength);
    if (internal_value_str->size() >= kListsMetaValueSuffixLength) {
      user_value_ =
          rocksdb::Slice(internal_value_str->data(), internal_value_str->size() - kListsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_str->data() + internal_value_str->size() - sizeof(int32_t) * 2 -
                               sizeof(int64_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_str->data() + internal_value_str->size() - sizeof(int32_t) -
                                 sizeof(int64_t) * 2);
      left_index_ = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(int64_t) * 2);
      right_index_ = DecodeFixed64(internal_value_str->data() + internal_value_str->size() - sizeof(int64_t));
    }
    count_ = DecodeFixed64(internal_value_str->data());
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const rocksdb::Slice& internal_value_slice)
      : ParsedInternalValue(internal_value_slice), count_(0), left_index_(0), right_index_(0) {
    assert(internal_value_slice.size() >= kListsMetaValueSuffixLength);
    if (internal_value_slice.size() >= kListsMetaValueSuffixLength) {
      user_value_ =
          rocksdb::Slice(internal_value_slice.data(), internal_value_slice.size() - kListsMetaValueSuffixLength);
      version_ = DecodeFixed32(internal_value_slice.data() + internal_value_slice.size() - sizeof(int32_t) * 2 -
                               sizeof(int64_t) * 2);
      timestamp_ = DecodeFixed32(internal_value_slice.data() + internal_value_slice.size() - sizeof(int32_t) -
                                 sizeof(int64_t) * 2);
      left_index_ = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(int64_t) * 2);
      right_index_ = DecodeFixed64(internal_value_slice.data() + internal_value_slice.size() - sizeof(int64_t));
    }
    count_ = DecodeFixed64(internal_value_slice.data());
  }

  void StripSuffix() override {
    if (value_ != nullptr) {
      value_->erase(value_->size() - kListsMetaValueSuffixLength, kListsMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength;
      EncodeFixed32(dst, version_);
    }
  }

  void SetTimestampToValue() override {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int32_t) - 2 * sizeof(int64_t);
      EncodeFixed32(dst, timestamp_);
    }
  }

  void SetIndexToValue() {
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
      dst += sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  static const size_t kListsMetaValueSuffixLength = 2 * sizeof(int32_t) + 2 * sizeof(int64_t);

  int32_t InitialMetaValue() {
    this->set_count(0);
    this->set_left_index(InitalLeftIndex);
    this->set_right_index(InitalRightIndex);
    this->set_timestamp(0);
    return this->UpdateVersion();
  }

  uint64_t count() { return count_; }

  void set_count(uint64_t count) {
    count_ = count;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, count_);
    }
  }

  void ModifyCount(uint64_t delta) {
    count_ += delta;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst, count_);
    }
  }

  int32_t UpdateVersion() {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    if (version_ >= static_cast<int32_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<int32_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

  uint64_t left_index() { return left_index_; }

  void set_left_index(uint64_t index) {
    left_index_ = index;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
    }
  }

  void ModifyLeftIndex(uint64_t index) {
    left_index_ -= index;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * sizeof(int64_t);
      EncodeFixed64(dst, left_index_);
    }
  }

  uint64_t right_index() { return right_index_; }

  void set_right_index(uint64_t index) {
    right_index_ = index;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

  void ModifyRightIndex(uint64_t index) {
    right_index_ += index;
    if (value_ != nullptr) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - sizeof(int64_t);
      EncodeFixed64(dst, right_index_);
    }
  }

 private:
  uint64_t count_;
  uint64_t left_index_;
  uint64_t right_index_;
};

}  //  namespace storage
#endif  //  SRC_LISTS_META_VALUE_FORMAT_H_
