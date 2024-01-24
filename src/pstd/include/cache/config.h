// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#ifndef __CACHE_CONFIG_H__
#define __CACHE_CONFIG_H__

#include <cstdint>

namespace cache {

/* Redis maxmemory strategies */
enum RedisMaxmemoryPolicy {
  CACHE_VOLATILE_LRU      = 0,
  CACHE_ALLKEYS_LRU       = 1,
  CACHE_VOLATILE_LFU      = 2,
  CACHE_ALLKEYS_LFU       = 3,
  CACHE_VOLATILE_RANDOM   = 4,
  CACHE_ALLKEYS_RANDOM    = 5,
  CACHE_VOLATILE_TTL      = 6,
  CACHE_NO_EVICTION       = 7
};

#define CACHE_DEFAULT_MAXMEMORY         ((uint64_t)(10) << 30) // 10G
#define CACHE_DEFAULT_MAXMEMORY_SAMPLES 5
#define CACHE_DEFAULT_LFU_DECAY_TIME    1

/*
 * cache start pos
 */
constexpr int CACHE_START_FROM_BEGIN = 0;
constexpr int CACHE_START_FROM_END = -1;
/*
 * cache items per key
 */
#define DEFAULT_CACHE_ITEMS_PER_KEY 512

struct CacheConfig {
  uint64_t maxmemory;                      /* Can used max memory */
  int32_t  maxmemory_policy;               /* Policy for key eviction */
  int32_t  maxmemory_samples;              /* Precision of random sampling */
  int32_t  lfu_decay_time;                 /* LFU counter decay factor. */
  int32_t  zset_cache_start_pos;
  int32_t  zset_cache_field_num_per_key;

  CacheConfig()
    : maxmemory(CACHE_DEFAULT_MAXMEMORY)
      , maxmemory_policy(CACHE_NO_EVICTION)
      , maxmemory_samples(CACHE_DEFAULT_MAXMEMORY_SAMPLES)
      , lfu_decay_time(CACHE_DEFAULT_LFU_DECAY_TIME)
      , zset_cache_start_pos(CACHE_START_FROM_BEGIN)
      , zset_cache_field_num_per_key(DEFAULT_CACHE_ITEMS_PER_KEY){}

  CacheConfig& operator=(const CacheConfig& obj) {
    maxmemory = obj.maxmemory;
    maxmemory_policy = obj.maxmemory_policy;
    maxmemory_samples = obj.maxmemory_samples;
    lfu_decay_time = obj.lfu_decay_time;
    zset_cache_start_pos = obj.zset_cache_start_pos;
    zset_cache_field_num_per_key = obj.zset_cache_field_num_per_key;
    return *this;
  }
};

} // namespace cache

#endif
