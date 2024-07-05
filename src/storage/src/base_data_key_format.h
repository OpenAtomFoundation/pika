//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_DATA_KEY_FORMAT_H_
#define SRC_BASE_DATA_KEY_FORMAT_H_

#include "src/coding.h"
#include "storage/storage_define.h"

namespace storage {

using Slice = rocksdb::Slice;
/*
* used for Hash/Set/Zset's member data key. format:
* | reserve1 | key | version | data | reserve2 |
* |    8B    |     |    8B   |      |   16B    |
*/
class BaseDataKey {
 public:
  BaseDataKey(const Slice& key,
             uint64_t version, const Slice& data)
      : key_(key), version_(version), data_(data) {}

  ~BaseDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice EncodeSeekKey() {
    size_t meta_size = sizeof(reserve1_) + sizeof(version_);
    size_t usize = key_.size() + data_.size() + kEncodedKeyDelimSize;
    size_t nzero = std::count(key_.data(), key_.data() + key_.size(), kNeedTransformCharacter);
    usize += nzero;
    size_t needed = meta_size + usize;
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
    // reserve1: 8 byte
    memcpy(dst, reserve1_, sizeof(reserve1_));
    dst += sizeof(reserve1_);
    // key
    dst = EncodeUserKey(key_, dst, nzero);
    // version 8 byte
    EncodeFixed64(dst, version_);
    dst += sizeof(version_);
    // data
    memcpy(dst, data_.data(), data_.size());
    dst += data_.size();
    return Slice(start_, needed);
  }

  Slice Encode() {
    size_t meta_size = sizeof(reserve1_) + sizeof(version_) + sizeof(reserve2_);
    size_t usize = key_.size() + data_.size() + kEncodedKeyDelimSize;
    size_t nzero = std::count(key_.data(), key_.data() + key_.size(), kNeedTransformCharacter);
    usize += nzero;
    size_t needed = meta_size + usize;
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
    // reserve1: 8 byte
    memcpy(dst, reserve1_, sizeof(reserve1_));
    dst += sizeof(reserve1_);
    // key
    dst = EncodeUserKey(key_, dst, nzero);
    // version 8 byte
    EncodeFixed64(dst, version_);
    dst += sizeof(version_);
    // data
    memcpy(dst, data_.data(), data_.size());
    dst += data_.size();
    // TODO(wangshaoyi): too much for reserve
    // reserve2: 16 byte
    memcpy(dst, reserve2_, sizeof(reserve2_));
    return Slice(start_, needed);
  }

 private:
  char* start_ = nullptr;
  char space_[200];
  char reserve1_[8] = {0};
  Slice key_;
  uint64_t version_ = uint64_t(-1);
  Slice data_;
  char reserve2_[16] = {0};
};

class ParsedBaseDataKey {
 public:
  explicit ParsedBaseDataKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedBaseDataKey(const Slice& key) {
    const char* ptr = key.data();
    const char* end_ptr = key.data() + key.size();
    decode(ptr, end_ptr);
  }

  void decode(const char* ptr, const char* end_ptr) {
    const char* start = ptr;
    // skip head reserve1_
    ptr += sizeof(reserve1_);
    // skip tail reserve2_
    end_ptr -= kSuffixReserveLength;
    // user key
    ptr = DecodeUserKey(ptr, std::distance(ptr, end_ptr), &key_str_);

    version_ = DecodeFixed64(ptr);
    ptr += sizeof(version_);
    data_ = Slice(ptr, std::distance(ptr, end_ptr));
  }

  virtual ~ParsedBaseDataKey() = default;

  Slice Key() { return Slice(key_str_); }

  uint64_t Version() { return version_; }

  Slice Data() { return data_; }

 protected:
  std::string key_str_;
  char reserve1_[8] = {0};
  uint64_t version_ = (uint64_t)(-1);
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

class ParsedStreamDataKey : public ParsedBaseDataKey {
 public:
  explicit ParsedStreamDataKey(const std::string* key) : ParsedBaseDataKey(key) {}
  explicit ParsedStreamDataKey(const Slice& key) : ParsedBaseDataKey(key) {}
  Slice id() { return data_; }
};

using HashesDataKey = BaseDataKey;
using SetsMemberKey = BaseDataKey;
using ZSetsMemberKey = BaseDataKey;
using StreamDataKey = BaseDataKey;

}  //  namespace storage
#endif  // SRC_BASE_DATA_KEY_FORMAT_H_
