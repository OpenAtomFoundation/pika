//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_VALUE_FORMAT_H_
#define SRC_BASE_VALUE_FORMAT_H_

#include <string>

#include "src/coding.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"

namespace blackwidow {

class InternalValue {
 public:
  explicit InternalValue(const Slice& user_value) :
    start_(nullptr),
    user_value_(user_value),
    version_(0),
    timestamp_(0) {
  }
  virtual ~InternalValue() {
    if (start_ != space_) {
      delete[] start_;
    }
  }
  void set_timestamp(int32_t timestamp = 0) {
    timestamp_ = timestamp;
  }
  void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
  }
  void set_version(int32_t version = 0) {
    version_ = version;
  }
  static const size_t kDefaultValueSuffixLength = sizeof(int32_t) * 2;
  virtual const Slice Encode() {
    size_t usize = user_value_.size();
    size_t needed = usize + kDefaultValueSuffixLength;
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
    size_t len = AppendTimestampAndVersion();
    return Slice(start_, len);
  }
  virtual size_t AppendTimestampAndVersion() = 0;

 protected:
  char space_[200];
  char* start_;
  Slice user_value_;
  int32_t version_;
  int32_t timestamp_;
};

class ParsedInternalValue {
 public:
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedInternalValue(std::string* value) :
    value_(value),
    version_(0),
    timestamp_(0) {
  }

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedInternalValue(const Slice& value) :
    value_(nullptr),
    version_(0),
    timestamp_(0) {
  }

  virtual ~ParsedInternalValue() = default;

  Slice user_value() {
    return user_value_;
  }

  int32_t version() {
    return version_;
  }

  void set_version(int32_t version) {
    version_ = version;
    SetVersionToValue();
  }

  int32_t timestamp() {
    return timestamp_;
  }

  void set_timestamp(int32_t timestamp) {
    timestamp_ = timestamp;
    SetTimestampToValue();
  }

  void SetRelativeTimestamp(int32_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    timestamp_ = static_cast<int32_t>(unix_time) + ttl;
    SetTimestampToValue();
  }

  bool IsPermanentSurvival() {
    return timestamp_ == 0;
  }

  bool IsStale() {
    if (timestamp_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    return timestamp_ < unix_time;
  }

  virtual void StripSuffix() = 0;

 protected:
  virtual void SetVersionToValue() = 0;
  virtual void SetTimestampToValue() = 0;
  std::string* value_;
  Slice user_value_;
  int32_t version_;
  int32_t timestamp_;
};


}  //  namespace blackwidow
#endif  // SRC_BASE_VALUE_FORMAT_H_
