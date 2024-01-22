//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_DATA_KEY_FORMAT_H_
#define SRC_LISTS_DATA_KEY_FORMAT_H_

#include "src/coding.h"
#include "storage/storage_define.h"

namespace storage {
/*
* used for List data key. format:
* | reserve1 | key | version | index | reserve2 |
* |    8B    |     |    8B   |   8B  |   16B    |
*/
class ListsDataKey {
public:
  ListsDataKey(const Slice& key, uint64_t version, uint64_t index)
      : key_(key), version_(version), index_(index) {}

  ~ListsDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t meta_size = sizeof(reserve1_) + sizeof(version_) + sizeof(reserve2_);
    size_t usize = key_.size() + sizeof(index_) + kEncodedKeyDelimSize;
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
    dst = EncodeUserKey(key_, dst, nzero);
    // version 8 byte
    EncodeFixed64(dst, version_);
    dst += sizeof(version_);
    // index
    EncodeFixed64(dst, index_);
    dst += sizeof(index_);
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
  uint64_t index_ = 0;
  char reserve2_[16] = {0};
};

class ParsedListsDataKey {
 public:
  explicit ParsedListsDataKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedListsDataKey(const Slice& key) {
    const char* ptr = key.data();
    const char* end_ptr = key.data() + key.size();
    decode(ptr, end_ptr);
  }

  void decode(const char* ptr, const char* end_ptr) {
    const char* start = ptr;
    // skip head reserve1_
    ptr += sizeof(reserve1_);
    // skip tail reserve2_
    end_ptr -= sizeof(reserve2_);

    ptr = DecodeUserKey(ptr, std::distance(ptr, end_ptr), &key_str_);
    version_ = DecodeFixed64(ptr);
    ptr += sizeof(version_);
    index_ = DecodeFixed64(ptr);
  }

  virtual ~ParsedListsDataKey() = default;

  Slice key() { return Slice(key_str_); }

  uint64_t Version() { return version_; }

  uint64_t index() { return index_; }

 private:
  std::string key_str_;
  char reserve1_[8] = {0};
  uint64_t version_ = (uint64_t)(-1);
  uint64_t index_ = 0;
  char reserve2_[16] = {0};
};

}  //  namespace storage
#endif  // SRC_LISTS_DATA_KEY_FORMAT_H_
