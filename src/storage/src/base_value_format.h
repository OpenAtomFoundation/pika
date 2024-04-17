//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_VALUE_FORMAT_H_
#define SRC_BASE_VALUE_FORMAT_H_

#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "src/coding.h"
#include "src/mutex.h"

#include "pstd/include/env.h"

namespace storage {

enum class Type : uint8_t { kString = 0, kHash = 1, kList = 2, kSet = 3, kZset = 4, kNulltype = 5 };

class InternalValue {
public:
  explicit InternalValue(Type type, const Slice& user_value) : type_(type), user_value_(user_value) { ctime_ = pstd::NowMicros() / 1000000;
  }
  virtual ~InternalValue() {
    if (start_ != space_) {
      delete[] start_;
    }
  }
  void SetEtime(uint64_t etime = 0) { etime_ = etime; }
  void setCtime(uint64_t ctime) { ctime_ = ctime; }
  rocksdb::Status SetRelativeTimestamp(int64_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    etime_ = uint64_t(unix_time + ttl);
    return rocksdb::Status::OK();
  }
  void SetVersion(uint64_t version = 0) { version_ = version; }

  char* ReAllocIfNeeded(size_t needed) {
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
    return dst;
  }

  virtual rocksdb::Slice Encode() = 0;

protected:
  char space_[200];
  char* start_ = nullptr;
  rocksdb::Slice user_value_;
  Type type_;
  uint64_t version_ = 0;
  uint64_t etime_ = 0;
  uint64_t ctime_ = 0;
  char reserve_[16] = {0};
};

class ParsedInternalValue {
public:
  // Use this constructor after rocksdb::DB::Get(), since we use this in
  // the implement of user interfaces and may need to modify the
  // original value suffix, so the value_ must point to the string
  explicit ParsedInternalValue(std::string* value) : value_(value) {}

  // Use this constructor in rocksdb::CompactionFilter::Filter(),
  // since we use this in Compaction process, all we need to do is parsing
  // the rocksdb::Slice, so don't need to modify the original value, value_ can be
  // set to nullptr
  explicit ParsedInternalValue(const rocksdb::Slice& value) {}

  virtual ~ParsedInternalValue() = default;

  rocksdb::Slice UserValue() { return user_value_; }

  uint64_t Version() { return version_; }

  void SetVersion(uint64_t version) {
    version_ = version;
    SetVersionToValue();
  }

  uint64_t Etime() { return etime_; }

  void SetEtime(uint64_t etime) {
    etime_ = etime;
    SetEtimeToValue();
  }

  void SetCtime(uint64_t ctime) {
    ctime_ = ctime;
    SetCtimeToValue();
  }

  void SetRelativeTimestamp(int64_t ttl) {
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    etime_ = unix_time + ttl;
    SetEtimeToValue();
  }

  bool IsPermanentSurvival() { return etime_ == 0; }

  bool IsStale() {
    if (etime_ == 0) {
      return false;
    }
    int64_t unix_time;
    rocksdb::Env::Default()->GetCurrentTime(&unix_time);
    return etime_ < unix_time;
  }

  virtual bool IsValid() {
    return !IsStale();
  }

  virtual void StripSuffix() = 0;
  bool IsSameType(const Type type) { return type_ == type; }

protected:
  virtual void SetVersionToValue() = 0;
  virtual void SetEtimeToValue() = 0;
  virtual void SetCtimeToValue() = 0;
  std::string* value_ = nullptr;
  rocksdb::Slice user_value_;
  uint64_t version_ = 0 ;
  uint64_t ctime_ = 0;
  uint64_t etime_ = 0;
  Type type_;
  char reserve_[16] = {0}; //unused
};

}  //  namespace storage
#endif  // SRC_BASE_VALUE_FORMAT_H_
