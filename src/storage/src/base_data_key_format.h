//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_DATA_KEY_FORMAT_H_
#define SRC_BASE_DATA_KEY_FORMAT_H_

#include "pstd/include/pstd_coding.h"

namespace storage {
class BaseDataKey {
 public:
  BaseDataKey(const Slice& key, int32_t version, const Slice& data)
      : start_(nullptr), key_(key), version_(version), data_(data) {}

  ~BaseDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t usize = key_.size() + data_.size();
    size_t needed = usize + sizeof(int32_t) * 2;
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
    pstd::EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    pstd::EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    memcpy(dst, data_.data(), data_.size());
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_ = nullptr;
  Slice key_;
  int32_t version_ = -1;
  Slice data_;
};

class ParsedBaseDataKey {
 public:
  explicit ParsedBaseDataKey(const std::string* key) {
    const char* ptr = key->data();
    int32_t key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    data_ = Slice(ptr, key->size() - key_len - sizeof(int32_t) * 2);
  }

  explicit ParsedBaseDataKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = pstd::DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    data_ = Slice(ptr, key.size() - key_len - sizeof(int32_t) * 2);
  }

  virtual ~ParsedBaseDataKey() = default;

  Slice key() { return key_; }

  int32_t version() { return version_; }

  Slice data() { return data_; }

 protected:
  Slice key_;
  int32_t version_ = -1;
  Slice data_;
};

class ParsedHashesDataKey : public ParsedBaseDataKey {
 public:
  explicit ParsedHashesDataKey(const std::string* key) : ParsedBaseDataKey(key) {}
  explicit ParsedHashesDataKey(const Slice& key) : ParsedBaseDataKey(key) {}
  Slice field() { return data_; }
};

class ParsedSetsMemberKey : public ParsedBaseDataKey {
 public:
  explicit ParsedSetsMemberKey(const std::string* key) : ParsedBaseDataKey(key) {}
  explicit ParsedSetsMemberKey(const Slice& key) : ParsedBaseDataKey(key) {}
  Slice member() { return data_; }
};

class ParsedZSetsMemberKey : public ParsedBaseDataKey {
 public:
  explicit ParsedZSetsMemberKey(const std::string* key) : ParsedBaseDataKey(key) {}
  explicit ParsedZSetsMemberKey(const Slice& key) : ParsedBaseDataKey(key) {}
  Slice member() { return data_; }
};

typedef BaseDataKey HashesDataKey;
typedef BaseDataKey SetsMemberKey;
typedef BaseDataKey ZSetsMemberKey;

}  //  namespace storage
#endif  // SRC_BASE_DATA_KEY_FORMAT_H_
