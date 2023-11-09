// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CACHE_MANAGER_H
#define PIKA_CACHE_MANAGER_H

#include <unordered_map>
#include <memory>

#include "include/pika_cache.h"

class PikaCacheManager : public pstd::noncopyable {
 public:
  PikaCacheManager();
  ~PikaCacheManager() = default;
  void Init(const std::map<std::string, std::shared_ptr<DB>>& dbs);
  void ClearHitRatio();
  PikaCache::CacheInfo Info();
  // for cache info
  uint64_t cache_usage_;
  struct DisplayCacheInfo {
    int status = 0;
    uint32_t cache_num = 0;
    uint64_t keys_num = 0;
    uint64_t used_memory = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t hits_per_sec = 0;
    uint64_t read_cmd_per_sec = 0;
    double hitratio_per_sec = 0.0;
    double hitratio_all = 0.0;
    uint64_t load_keys_per_sec = 0;
    uint64_t last_time_us = 0;
    uint64_t last_load_keys_num = 0;
    uint32_t waitting_load_keys_num = 0;
    DisplayCacheInfo& operator=(const DisplayCacheInfo &obj) {
      status = obj.status;
      cache_num = obj.cache_num;
      keys_num = obj.keys_num;
      used_memory = obj.used_memory;
      hits = obj.hits;
      misses = obj.misses;
      hits_per_sec = obj.hits_per_sec;
      read_cmd_per_sec = obj.read_cmd_per_sec;
      hitratio_per_sec = obj.hitratio_per_sec;
      hitratio_all = obj.hitratio_all;
      load_keys_per_sec = obj.load_keys_per_sec;
      last_time_us = obj.last_time_us;
      last_load_keys_num = obj.last_load_keys_num;
      waitting_load_keys_num = obj.waitting_load_keys_num;
      return *this;
    }
  };
  void UpdateCacheInfo(void);
  void GetCacheInfo(DisplayCacheInfo &cache_info);
  void ResetDisplayCacheInfo(int status);
  void CacheConfigInit(cache::CacheConfig &cache_cfg);
 private:
  std::shared_mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<PikaCache>> caches_;
  std::atomic<int> cache_status_;
  PikaCacheLoadThread *cache_load_thread_;
  DisplayCacheInfo cache_info_;
  std::shared_mutex cache_info_rwlock_;
};


#endif
