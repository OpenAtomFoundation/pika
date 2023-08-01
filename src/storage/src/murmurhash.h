//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

/*
  Murmurhash from http://sites.google.com/site/murmurhash/

  All code is released to the public domain. For business purposes, Murmurhash
  is under the MIT license.
*/
#ifndef SRC_MURMURHASH_H_
#define SRC_MURMURHASH_H_

#include <cstdint>
#include "rocksdb/slice.h"

#if defined(__x86_64__)
#  define MURMUR_HASH MurmurHash64A
uint64_t MurmurHash64A(const void* key, int32_t len, uint32_t seed);
#  define MurmurHash MurmurHash64A
typedef uint64_t murmur_t;

#elif defined(__i386__)
#  define MURMUR_HASH MurmurHash2
uint32_t MurmurHash2(const void* key, int32_t len, uint32_t seed);
#  define MurmurHash MurmurHash2
typedef uint32_t murmur_t;

#else
#  define MURMUR_HASH MurmurHashNeutral2
uint32_t MurmurHashNeutral2(const void* key, int32_t len, uint32_t seed);
#  define MurmurHash MurmurHashNeutral2
using murmur_t = uint32_t;
#endif

// Allow slice to be hashable by murmur hash.
namespace storage {
using Slice = rocksdb::Slice;
struct murmur_hash {
  size_t operator()(const Slice& slice) const { return MurmurHash(slice.data(), static_cast<int32_t>(slice.size()), 0); }
};
}  // namespace storage
#endif  // SRC_MURMURHASH_H_
