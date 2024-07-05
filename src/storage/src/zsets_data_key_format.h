//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_DATA_KEY_FORMAT_H_
#define SRC_ZSETS_DATA_KEY_FORMAT_H_

#include "src/coding.h"
#include "storage/storage_define.h"

namespace storage {

/* zset score to member data key format:
* | reserve1 | key | version | score | member |  reserve2 |
* |    8B    |     |    8B   |  8B   |        |    16B    |
 */
class ZSetsScoreKey {
 public:
  ZSetsScoreKey(const Slice& key, uint64_t version,
                double score, const Slice& member)
      : key_(key), version_(version),
        score_(score), member_(member) {}

  ~ZSetsScoreKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t meta_size = sizeof(reserve1_) + sizeof(version_) + sizeof(score_) + sizeof(reserve2_);
    size_t usize = key_.size() + member_.size() + kEncodedKeyDelimSize;
    size_t nzero = std::count(key_.data(), key_.data() + key_.size(), kNeedTransformCharacter);
    usize += nzero;
    size_t needed = meta_size + usize;
    char* dst = nullptr;
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
    // score
    const void* addr_score = reinterpret_cast<const void*>(&score_);
    EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_score));
    dst += sizeof(score_);
    // member
    memcpy(dst, member_.data(), member_.size());
    dst += member_.size();
    // reserve2 16 byte
    memcpy(dst, reserve2_, sizeof(reserve2_));
    return Slice(start_, needed);
  }

 private:
  char* start_ = nullptr;
  char space_[200];
  char reserve1_[8] = {0};
  Slice key_;
  uint64_t version_ = uint64_t(-1);
  double score_ = 0.0;
  Slice member_;
  char reserve2_[16] = {0};
};

class ParsedZSetsScoreKey {
 public:
  explicit ParsedZSetsScoreKey(const std::string* key) {
    const char* ptr = key->data();
    const char* end_ptr = key->data() + key->size();
    decode(ptr, end_ptr);
  }

  explicit ParsedZSetsScoreKey(const Slice& key) {
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
    // user key
    ptr = DecodeUserKey(ptr, std::distance(ptr, end_ptr), &key_str_);
    version_ = DecodeFixed64(ptr);
    ptr += sizeof(version_);
    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
    member_ = Slice(ptr, std::distance(ptr, end_ptr));
  }

  Slice key() { return Slice(key_str_); }
  uint64_t Version() const { return version_; }
  double score() const { return score_; }
  Slice member() { return member_; }

 private:
  std::string key_str_;
  char reserve1_[8] = {0};
  uint64_t version_ = uint64_t(-1);
  char reserve2_[16] = {0};
  double score_ = 0.0;
  Slice member_;
};

}  // namespace storage
#endif  // SRC_ZSETS_DATA_KEY_FORMAT_H_
