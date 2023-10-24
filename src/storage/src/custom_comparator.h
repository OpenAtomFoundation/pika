//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CUSTOM_COMPARATOR_H_
#define INCLUDE_CUSTOM_COMPARATOR_H_

#include "rocksdb/comparator.h"
#include "glog/logging.h"

#include "storage/storage_define.h"
#include "src/debug.h"
#include "src/coding.h"

namespace storage {
/* list data key pattern
* | reserve1 | key | version | index | reserve2 |
* |    8B    |     |    8B   |  8B   |   16B    |
*/
class ListsDataKeyComparatorImpl : public rocksdb::Comparator {
 public:
  ListsDataKeyComparatorImpl() = default;

  // keep compatible with floyd
  const char* Name() const override { return "floyd.ListsDataKeyComparator"; }

  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    assert(!a.empty() && !b.empty());
    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    auto a_size = static_cast<int32_t>(a.size());
    auto b_size = static_cast<int32_t>(b.size());

    ptr_a += kPrefixReserveLength;
    ptr_b += kPrefixReserveLength;
    ptr_a = SeekUserkeyDelim(ptr_a, a_size - kPrefixReserveLength);
    ptr_b = SeekUserkeyDelim(ptr_b, b_size - kPrefixReserveLength);

    rocksdb::Slice a_prefix(a.data(), std::distance(a.data(), ptr_a));
    rocksdb::Slice b_prefix(b.data(), std::distance(b.data(), ptr_b));
    if (a_prefix != b_prefix) {
      return a_prefix.compare(b_prefix);
    }

    if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t version_a = DecodeFixed64(ptr_a);
    uint64_t version_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (version_a != version_b) {
      return version_a < version_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t index_a = DecodeFixed64(ptr_a);
    uint64_t index_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (index_a != index_b) {
      return index_a < index_b ? -1 : 1;
    } else {
      return 0;
    }
  }

  bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const override { return Compare(a, b) == 0; }

  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {}

  void FindShortSuccessor(std::string* key) const override {}
};

/* zset score key pattern
 *  | <Reserve 1> |      <Key>      |  <Version>  |  <Score>  | <Member> | <Reserve2> |
 *  |   8 Bytes   |  Key Size Bytes |   8 Bytes   |  8 Bytes  |          |     16B    |
 */
class ZSetsScoreKeyComparatorImpl : public rocksdb::Comparator {
 public:
  // keep compatible with floyd
  const char* Name() const override { return "floyd.ZSetsScoreKeyComparator"; }
  int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const override {
    assert(a.size() > kPrefixReserveLength);
    assert(b.size() > kPrefixReserveLength);

    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    auto a_size = static_cast<int32_t>(a.size());
    auto b_size = static_cast<int32_t>(b.size());

    // compare reserve, current always equal
    int ret = memcmp(ptr_a, ptr_b, kPrefixReserveLength);
    if (ret) return ret;

    ptr_a += kPrefixReserveLength;
    ptr_b += kPrefixReserveLength;
    std::string user_key_a, user_key_b;
    ptr_a = DecodeUserKey(ptr_a, a_size - kPrefixReserveLength, &user_key_a);
    ptr_b = DecodeUserKey(ptr_b, b_size - kPrefixReserveLength, &user_key_b);
    // compare user key
    ret = user_key_a.compare(user_key_b);
    if (ret) return ret;

    // compare version
    ret = memcmp(ptr_a, ptr_b, kVersionLength);
    if (ret) return ret;

    // compare score
    ptr_a += kVersionLength;
    ptr_b += kVersionLength;
    uint64_t a_i = DecodeFixed64(ptr_a);
    uint64_t b_i = DecodeFixed64(ptr_b);
    const void* ptr_a_score = reinterpret_cast<const void*>(&a_i);
    const void* ptr_b_score = reinterpret_cast<const void*>(&b_i);
    double a_score = *reinterpret_cast<const double*>(ptr_a_score);
    double b_score = *reinterpret_cast<const double*>(ptr_b_score);
    if (a_score != b_score) {
      return a_score < b_score ? -1 : 1;
    }

    // compare rest of the key, including: member and reserve
    ptr_a += kScoreLength;
    ptr_b += kScoreLength;
    rocksdb::Slice rest_a(ptr_a, a_size - std::distance(a.data(), ptr_a));
    rocksdb::Slice rest_b(ptr_b, b_size - std::distance(b.data(), ptr_b));
    return rest_a.compare(rest_b);
  }

  bool Equal(const rocksdb::Slice& a, const rocksdb::Slice& b) const override { return Compare(a, b) == 0; }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  // TODO(wangshaoyi): need reformat, if pkey differs, why return limit directly?
  void FindShortestSeparator(std::string* start, const rocksdb::Slice& limit) const override {
    assert(start->size() > kPrefixReserveLength);
    assert(limit.size() > kPrefixReserveLength);

    const char* head_start = start->data();
    const char* head_limit = limit.data();
    const char* ptr_start = start->data();
    const char* ptr_limit = limit.data();
    ptr_start += kPrefixReserveLength;
    ptr_limit += kPrefixReserveLength;
    ptr_start = SeekUserkeyDelim(ptr_start, start->size() - std::distance(head_start, ptr_start));
    ptr_limit = SeekUserkeyDelim(ptr_limit, limit.size() - std::distance(head_limit, ptr_limit));

    ptr_start += kVersionLength;
    ptr_limit += kVersionLength;

    size_t start_head_to_version_length = std::distance(head_start, ptr_start);
    size_t limit_head_to_version_length = std::distance(head_limit, ptr_limit);

    rocksdb::Slice key_start_prefix(start->data(), start_head_to_version_length);
    rocksdb::Slice key_limit_prefix(start->data(), limit_head_to_version_length);
    if (key_start_prefix.compare(key_limit_prefix) != 0) {
      return;
    }

    uint64_t start_i = DecodeFixed64(ptr_start);
    uint64_t limit_i = DecodeFixed64(ptr_limit);
    const void* ptr_start_score = reinterpret_cast<const void*>(&start_i);
    const void* ptr_limit_score = reinterpret_cast<const void*>(&limit_i);
    double start_score = *reinterpret_cast<const double*>(ptr_start_score);
    double limit_score = *reinterpret_cast<const double*>(ptr_limit_score);
    ptr_start += sizeof(uint64_t);
    ptr_limit += sizeof(uint64_t);
    if (start_score < limit_score) {
      if (start_score + 1 < limit_score) {
        start->resize(start_head_to_version_length);
        start_score += 1;
        const void* addr_start_score = reinterpret_cast<const void*>(&start_score);
        char dst[sizeof(uint64_t)];
        EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_start_score));
        start->append(dst, sizeof(uint64_t));
      }
      return;
    }

    size_t head_to_score_length = start_head_to_version_length + kScoreLength;

    std::string start_rest(ptr_start, start->size() - head_to_score_length);
    std::string limit_rest(ptr_limit, limit.size() - head_to_score_length);
    // Find length of common prefix
    size_t min_length = std::min(start_rest.size(), limit_rest.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) && (start_rest[diff_index] == limit_rest[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      auto key_start_member_byte = static_cast<uint8_t>(start_rest[diff_index]);
      auto key_limit_member_byte = static_cast<uint8_t>(limit_rest[diff_index]);
      if (key_start_member_byte >= key_limit_member_byte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(key_start_member_byte < key_limit_member_byte);

      if (diff_index < limit_rest.size() - 1 || key_start_member_byte + 1 < key_limit_member_byte) {
        start_rest[diff_index]++;
        start_rest.resize(diff_index + 1);
        start->resize(head_to_score_length);
        start->append(start_rest);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < start_rest.size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>(start_rest[diff_index]) < static_cast<uint8_t>(0xff)) {
            start_rest[diff_index]++;
            start_rest.resize(diff_index + 1);
            start->resize(head_to_score_length);
            start->append(start_rest);
            break;
          }
          diff_index++;
        }
      }
    }
  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) const override {}
};

}  //  namespace storage
#endif  //  INCLUDE_CUSTOM_COMPARATOR_H_
