//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_DATA_KEY_FORMAT_H_
#define SRC_ZSETS_DATA_KEY_FORMAT_H_

namespace storage {

/*
 * |  <Key Size>  |      <Key>      | <Version> |  <Score>  |      <Member> | 4
 * Bytes      key size Bytes    4 Bytes     8 Bytes    member size Bytes
 */
class ZSetsScoreKey {
 public:
  ZSetsScoreKey(const Slice& key, int32_t version, double score,
                const Slice& member)
      : key_(key), version_(version), score_(score), member_(member) {}

  ~ZSetsScoreKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  Slice Encode() {
    size_t needed =
        key_.size() + member_.size() + sizeof(int32_t) * 2 + sizeof(uint64_t);
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
    EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    const void* addr_score = reinterpret_cast<const void*>(&score_);
    EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_score));
    dst += sizeof(uint64_t);
    memcpy(dst, member_.data(), member_.size());
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_ = nullptr;
  Slice key_;
  int32_t version_ = 0;
  double score_ = 0.0;
  Slice member_;
};

class ParsedZSetsScoreKey {
 public:
  explicit ParsedZSetsScoreKey(const std::string* key) {
    const char* ptr = key->data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
    member_ = Slice(
        ptr, key->size() - key_len - 2 * sizeof(int32_t) - sizeof(uint64_t));
  }

  explicit ParsedZSetsScoreKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
    member_ = Slice(
        ptr, key.size() - key_len - 2 * sizeof(int32_t) - sizeof(uint64_t));
  }

  Slice key() { return key_; }
  int32_t version() const { return version_; }
  double score() const { return score_; }
  Slice member() { return member_; }

 private:
  Slice key_;
  int32_t version_ = 0;
  double score_ = 0.0;
  Slice member_;
};

}  // namespace storage
#endif  // SRC_ZSETS_DATA_KEY_FORMAT_H_
