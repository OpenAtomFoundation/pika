//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

/*
  Murmurhash from http://sites.google.com/site/murmurhash/

  All code is released to the public domain. For business purposes, Murmurhash
  is under the MIT license.
*/
#include "src/murmurhash.h"

#if defined(__x86_64__)

// -------------------------------------------------------------------
//
// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.
//
// 64-bit hash for 64-bit platforms

uint64_t MurmurHash64A(const void* key, int32_t len, uint32_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int32_t r = 47;

  uint64_t h = seed ^ (len * m);

  auto data = static_cast<const uint64_t*>(key);
  auto end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  auto data2 = reinterpret_cast<const unsigned char*>(data);

  switch (len & 7) {
    case 7:
      h ^= (static_cast<uint64_t>(data2[6])) << 48;
      [[fallthrough]];
    case 6:
      h ^= (static_cast<uint64_t>(data2[5])) << 40;
      [[fallthrough]];
    case 5:
      h ^= (static_cast<uint64_t>(data2[4])) << 32;
      [[fallthrough]];
    case 4:
      h ^= (static_cast<uint64_t>(data2[3])) << 24;
      [[fallthrough]];
    case 3:
      h ^= (static_cast<uint64_t>(data2[2])) << 16;
      [[fallthrough]];
    case 2:
      h ^= (static_cast<uint64_t>(data2[1])) << 8;
      [[fallthrough]];
    case 1:
      h ^= (static_cast<uint64_t>(data2[0]));
      h *= m;
  }

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

#elif defined(__i386__)

// -------------------------------------------------------------------
//
// Note - This code makes a few assumptions about how your machine behaves -
//
// 1. We can read a 4-byte value from any address without crashing
// 2. sizeof(int32_t) == 4
//
// And it has a few limitations -
//
// 1. It will not work incrementally.
// 2. It will not produce the same results on little-endian and big-endian
//    machines.

uint32_t MurmurHash2(const void* key, int32_t len, uint32_t seed) {
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const uint32_t m = 0x5bd1e995;
  const int32_t r = 24;

  // Initialize the hash to a 'random' value

  uint32_t h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  auto data = (const unsigned char*)key;

  while (len >= 4) {
    uint32_t k = *(uint32_t*)data;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  // Handle the last few bytes of the input array

  switch (len) {
    case 3:
      h ^= data[2] << 16;
      [[fallthrough]];
    case 2:
      h ^= data[1] << 8;
      [[fallthrough]];
    case 1:
      h ^= data[0];
      h *= m;
  }

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

#else

// -------------------------------------------------------------------
//
// Same as MurmurHash2, but endian- and alignment-neutral.
// Half the speed though, alas.

uint32_t MurmurHashNeutral2(const void* key, int32_t len, uint32_t seed) {
  const uint32_t m = 0x5bd1e995;
  const int32_t r = 24;

  uint32_t h = seed ^ len;

  auto data = static_cast<const unsigned char*>(key);

  while (len >= 4) {
    uint32_t k;

    k = data[0];
    k |= data[1] << 8;
    k |= data[2] << 16;
    k |= data[3] << 24;

    k *= m;
    k ^= k >> r;
    k *= m;

    h *= m;
    h ^= k;

    data += 4;
    len -= 4;
  }

  switch (len) {
    case 3:
      h ^= data[2] << 16;
      [[fallthrough]];
    case 2:
      h ^= data[1] << 8;
      [[fallthrough]];
    case 1:
      h ^= data[0];
      h *= m;
  }

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
}

#endif
