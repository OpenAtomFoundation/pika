//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_META_VALUE_FORMAT_H_
#define SRC_LISTS_META_VALUE_FORMAT_H_

#include <string>

#include "src/base_value_format.h"
#include "storage/storage_define.h"

namespace storage {

const uint64_t InitalLeftIndex = 9223372036854775807;
const uint64_t InitalRightIndex = 9223372036854775808U;

/*
*| type | list_size | version | left index | right index | reserve |  cdate | timestamp |
*|  1B  |     8B    |    8B   |     8B     |      8B     |   16B   |    8B  |     8B    |
*/
class ListsMetaValue : public InternalValue {
 public:
  explicit ListsMetaValue(const rocksdb::Slice& user_value)
      : InternalValue(DataType::kLists, user_value), left_index_(InitalLeftIndex), right_index_(InitalRightIndex) {}

  rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kVersionLength + 2 * kListValueIndexLength +
                    kSuffixReserveLength + 2 * kTimestampLength + kTypeLength;
    char* dst = ReAllocIfNeeded(needed);
    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), usize);
    dst += usize;
    EncodeFixed64(dst, version_);
    dst += kVersionLength;
    EncodeFixed64(dst, left_index_);
    dst += kListValueIndexLength;
    EncodeFixed64(dst, right_index_);
    dst += kListValueIndexLength;
    memcpy(dst, reserve_, sizeof(reserve_));
    dst += kSuffixReserveLength;
    // The most significant bit is 1 for milliseconds and 0 for seconds.
    // The previous data was stored in seconds, but the subsequent data was stored in milliseconds
    uint64_t ctime = ctime_ > 0 ? (ctime_ | (1ULL << 63)) : 0;
    EncodeFixed64(dst, ctime);
    dst += kTimestampLength;
    uint64_t etime = etime_ > 0 ? (etime_ | (1ULL << 63)) : 0;
    EncodeFixed64(dst, etime);
    return {start_, needed};
  }

  uint64_t UpdateVersion() {
    pstd::TimeType unix_time = pstd::NowMillis();
    if (version_ >= static_cast<uint64_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<uint64_t>(unix_time);
    }
    return version_;
  }

  uint64_t LeftIndex() { return left_index_; }

  void ModifyLeftIndex(uint64_t index) { left_index_ -= index; }

  uint64_t RightIndex() { return right_index_; }

  void ModifyRightIndex(uint64_t index) { right_index_ += index; }

 private:
  uint64_t left_index_ = 0;
  uint64_t right_index_ = 0;
};

class ParsedListsMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedListsMetaValue(std::string* internal_value_str)
      : ParsedInternalValue(internal_value_str) {
    assert(internal_value_str->size() >= kListsMetaValueSuffixLength);
    if (internal_value_str->size() >= kListsMetaValueSuffixLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>((*internal_value_str)[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_str->data() + kTypeLength,
                                   internal_value_str->size() - kListsMetaValueSuffixLength - kTypeLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += kVersionLength;
      left_index_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += kListValueIndexLength;
      right_index_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += kListValueIndexLength;
      memcpy(reserve_, internal_value_str->data() + offset, sizeof(reserve_));
      offset += kSuffixReserveLength;
      uint64_t ctime = DecodeFixed64(internal_value_str->data() + offset);
      offset += kTimestampLength;
      uint64_t etime = DecodeFixed64(internal_value_str->data() + offset);
      offset += kTimestampLength;

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_==ctime, means ctime_ storaged in seconds
      if (ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_==etime, means etime_ storaged in seconds
      if (etime == etime_) {
        etime_ *= 1000;
      }
    }
    count_ = DecodeFixed64(internal_value_str->data() + kTypeLength);
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedListsMetaValue(const rocksdb::Slice& internal_value_slice)
      : ParsedInternalValue(internal_value_slice) {
    assert(internal_value_slice.size() >= kListsMetaValueSuffixLength);
    if (internal_value_slice.size() >= kListsMetaValueSuffixLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>(internal_value_slice[0]));
      offset += kTypeLength;
      user_value_ = rocksdb::Slice(internal_value_slice.data() + kTypeLength,
                                   internal_value_slice.size() - kListsMetaValueSuffixLength - kTypeLength);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kVersionLength;
      left_index_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kListValueIndexLength;
      right_index_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kListValueIndexLength;
      memcpy(reserve_, internal_value_slice.data() + offset, sizeof(reserve_));
      offset += kSuffixReserveLength;
      uint64_t ctime = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kTimestampLength;
      uint64_t etime = DecodeFixed64(internal_value_slice.data() + offset);
      offset += kTimestampLength;

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_==ctime, means ctime_ storaged in seconds
      if (ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_==etime, means etime_ storaged in seconds
      if (etime == etime_) {
        etime_ *= 1000;
      }
    }
    count_ = DecodeFixed64(internal_value_slice.data() + kTypeLength);
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kListsMetaValueSuffixLength, kListsMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength;
      EncodeFixed64(dst, version_);
    }
  }

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * kTimestampLength;
      uint64_t ctime = ctime_ > 0 ? (ctime_ | (1ULL << 63)) : 0;
      EncodeFixed64(dst, ctime);
    }
  }

  void SetEtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kTimestampLength;
      uint64_t etime = etime_ > 0 ? (etime_ | (1ULL << 63)) : 0;
      EncodeFixed64(dst, etime);
    }
  }

  void SetIndexToValue() {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength + kVersionLength;
      EncodeFixed64(dst, left_index_);
      dst += sizeof(left_index_);
      EncodeFixed64(dst, right_index_);
    }
  }

  uint64_t InitialMetaValue() {
    this->SetCount(0);
    this->set_left_index(InitalLeftIndex);
    this->set_right_index(InitalRightIndex);
    this->SetEtime(0);
    this->SetCtime(0);
    return this->UpdateVersion();
  }

  bool IsValid() override {
    return !IsStale() && Count() != 0;
  }

  uint64_t Count() { return count_; }

  void SetCount(uint64_t count) {
    count_ = count;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst + kTypeLength, count_);
    }
  }

  void ModifyCount(uint64_t delta) {
    count_ += delta;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed64(dst + kTypeLength, count_);
    }
  }

  uint64_t UpdateVersion() {
    pstd::TimeType unix_time = pstd::NowMillis();
    if (version_ >= static_cast<uint64_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<uint64_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

  uint64_t LeftIndex() { return left_index_; }

  void set_left_index(uint64_t index) {
    left_index_ = index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength + kVersionLength;
      EncodeFixed64(dst, left_index_);
    }
  }

  void ModifyLeftIndex(uint64_t index) {
    left_index_ -= index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength + kVersionLength;
      EncodeFixed64(dst, left_index_);
    }
  }

  uint64_t RightIndex() { return right_index_; }

  void set_right_index(uint64_t index) {
    right_index_ = index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength + kVersionLength + kListValueIndexLength;
      EncodeFixed64(dst, right_index_);
    }
  }

  void ModifyRightIndex(uint64_t index) {
    right_index_ += index;
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kListsMetaValueSuffixLength + kVersionLength + kListValueIndexLength;
      EncodeFixed64(dst, right_index_);
    }
  }

private:
  const size_t kListsMetaValueSuffixLength = kVersionLength + 2 * kListValueIndexLength + kSuffixReserveLength + 2 * kTimestampLength;

 private:
  uint64_t count_ = 0;
  uint64_t left_index_ = 0;
  uint64_t right_index_ = 0;
};

}  //  namespace storage
#endif  //  SRC_LISTS_META_VALUE_FORMAT_H_
