//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_META_VALUE_FORMAT_H_
#define SRC_BASE_META_VALUE_FORMAT_H_

#include <string>

#include "pstd/include/env.h"
#include "storage/storage_define.h"
#include "src/base_value_format.h"

namespace storage {

/*
*| type | value |  version | reserve | cdate | timestamp |
*|  1B  |       |    8B   |   16B    |   8B  |     8B    |
*/
// TODO(wangshaoyi): reformat encode, AppendTimestampAndVersion
class BaseMetaValue : public InternalValue {
 public:
  /*
   * Constructing MetaValue requires passing in a type value
   */
  explicit BaseMetaValue(DataType type, const Slice& user_value) : InternalValue(type, user_value) {}
  rocksdb::Slice Encode() override {
    size_t usize = user_value_.size();
    size_t needed = usize + kVersionLength + kSuffixReserveLength + 2 * kTimestampLength + kTypeLength;
    char* dst = ReAllocIfNeeded(needed);
    memcpy(dst, &type_, sizeof(type_));
    dst += sizeof(type_);
    char* start_pos = dst;

    memcpy(dst, user_value_.data(), user_value_.size());
    dst += user_value_.size();
    EncodeFixed64(dst, version_);
    dst += sizeof(version_);
    memcpy(dst, reserve_, sizeof(reserve_));
    dst += sizeof(reserve_);
    // The most significant bit is 1 for milliseconds and 0 for seconds.
    // The previous data was stored in seconds, but the subsequent data was stored in milliseconds
    uint64_t ctime = (ctime_ | (1ULL << 63));
    EncodeFixed64(dst, ctime);
    dst += sizeof(ctime_);
    uint64_t etime = (etime_ | (1ULL << 63));
    EncodeFixed64(dst, etime);
    return {start_, needed};
  }

  uint64_t UpdateVersion() {
    int64_t unix_time = pstd::NowMillis();
    if (version_ >= unix_time) {
      version_++;
    } else {
      version_ = uint64_t(unix_time);
    }
    return version_;
  }
};

class ParsedBaseMetaValue : public ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get();
  explicit ParsedBaseMetaValue(std::string* internal_value_str) : ParsedInternalValue(internal_value_str) {
    if (internal_value_str->size() >= kBaseMetaValueSuffixLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>((*internal_value_str)[0]));
      offset += kTypeLength;
      user_value_ = Slice(internal_value_str->data() + offset,
                             internal_value_str->size() - kBaseMetaValueSuffixLength - offset);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(version_);
      memcpy(reserve_, internal_value_str->data() + offset, sizeof(reserve_));
      offset += sizeof(reserve_);
      uint64_t ctime = DecodeFixed64(internal_value_str->data() + offset);
      offset += sizeof(ctime_);
      uint64_t etime = DecodeFixed64(internal_value_str->data() + offset);

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_==ctime, means ctime_ storaged in seconds
      if(ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_==etime, means etime_ storaged in seconds
      if(etime == etime_) {
        etime_ *= 1000;
      }
    }
    count_ = DecodeFixed32(internal_value_str->data() + kTypeLength);
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter();
  explicit ParsedBaseMetaValue(const Slice& internal_value_slice) : ParsedInternalValue(internal_value_slice) {
    if (internal_value_slice.size() >= kBaseMetaValueSuffixLength) {
      size_t offset = 0;
      type_ = static_cast<DataType>(static_cast<uint8_t>(internal_value_slice[0]));
      offset += kTypeLength;
      user_value_ = Slice(internal_value_slice.data() + offset,
                          internal_value_slice.size() - kBaseMetaValueSuffixLength - offset);
      offset += user_value_.size();
      version_ = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(uint64_t);
      memcpy(reserve_, internal_value_slice.data() + offset, sizeof(reserve_));
      offset += sizeof(reserve_);
      uint64_t ctime = DecodeFixed64(internal_value_slice.data() + offset);
      offset += sizeof(ctime_);
      uint64_t etime = DecodeFixed64(internal_value_slice.data() + offset);

      ctime_ = (ctime & ~(1ULL << 63));
      // if ctime_!=ctime, means ctime_ storaged in seconds
      if(ctime_ == ctime) {
        ctime_ *= 1000;
      }
      etime_ = (etime & ~(1ULL << 63));
      // if etime_!=etime, means etime_ storaged in seconds
      if(etime == etime_) {
        etime_ *= 1000;
      }
    }
    count_ = DecodeFixed32(internal_value_slice.data() + kTypeLength);
  }

  void StripSuffix() override {
    if (value_) {
      value_->erase(value_->size() - kBaseMetaValueSuffixLength, kBaseMetaValueSuffixLength);
    }
  }

  void SetVersionToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kBaseMetaValueSuffixLength;
      EncodeFixed64(dst, version_);
    }
  }

  void SetCtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - 2 * kTimestampLength;
      uint64_t ctime = (ctime_ | (1LL << 63));
      EncodeFixed64(dst, ctime);
    }
  }

  void SetEtimeToValue() override {
    if (value_) {
      char* dst = const_cast<char*>(value_->data()) + value_->size() - kTimestampLength;
      uint64_t etime = (etime_ | (1LL << 63));
      EncodeFixed64(dst, etime);
    }
  }

  uint64_t InitialMetaValue() {
    this->SetCount(0);
    this->SetEtime(0);
    this->SetCtime(0);
    return this->UpdateVersion();
  }

  bool IsValid() override {
    return !IsStale() && Count() != 0;
  }

  bool check_set_count(size_t count) {
    if (count > INT32_MAX) {
      return false;
    }
    return true;
  }

  int32_t Count() { return count_; }

  void SetCount(int32_t count) {
    count_ = count;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst + kTypeLength, count_);
    }
  }

  bool CheckModifyCount(int32_t delta) {
    int64_t count = count_;
    count += delta;
    if (count < 0 || count > INT32_MAX) {
      return false;
    }
    return true;
  }

  void ModifyCount(int32_t delta) {
    count_ += delta;
    if (value_) {
      char* dst = const_cast<char*>(value_->data());
      EncodeFixed32(dst + kTypeLength, count_);
    }
  }

  uint64_t UpdateVersion() {
    int64_t unix_time = pstd::NowMillis();
    if (version_ >= static_cast<uint64_t>(unix_time)) {
      version_++;
    } else {
      version_ = static_cast<uint64_t>(unix_time);
    }
    SetVersionToValue();
    return version_;
  }

 private:
  static const size_t kBaseMetaValueSuffixLength = kVersionLength + kSuffixReserveLength + 2 * kTimestampLength;
  int32_t count_ = 0;
};

using HashesMetaValue = BaseMetaValue;
using ParsedHashesMetaValue = ParsedBaseMetaValue;
using SetsMetaValue = BaseMetaValue;
using ParsedSetsMetaValue = ParsedBaseMetaValue;
using ZSetsMetaValue = BaseMetaValue;
using ParsedZSetsMetaValue = ParsedBaseMetaValue;

}  //  namespace storage
#endif  // SRC_BASE_META_VALUE_FORMAT_H_
