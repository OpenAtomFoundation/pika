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
  struct CacheInfo {
    int status;
    uint32_t cache_num;
    long long keys_num;
    size_t used_memory;
    long long hits;
    long long misses;
    uint64_t async_load_keys_num;
    uint32_t waitting_load_keys_num;
    CacheInfo()
        : status(PIKA_CACHE_STATUS_NONE),
          cache_num(0),
          keys_num(0),
          used_memory(0),
          hits(0),
          misses(0),
          async_load_keys_num(0),
          waitting_load_keys_num(0) {}
    void clear() {
      status = PIKA_CACHE_STATUS_NONE;
      cache_num = 0;
      keys_num = 0;
      used_memory = 0;
      hits = 0;
      misses = 0;
      async_load_keys_num = 0;
      waitting_load_keys_num = 0;
    }
  };

  PikaCacheManager();
  ~PikaCacheManager();
  std::shared_ptr<PikaCache> GetCache(const std::string& db_name, int slot_index);
  void Init(std::vector<DBStruct> dbs);
  void ProcessCronTask();
  void FlushDB(const std::string& db_name);
  void FlushAll();
  double HitRatio();
  void ClearHitRatio();
 private:
  std::shared_mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<PikaCache>> caches_;
  std::atomic<int> cache_status_;
  PikaCacheLoadThread *cache_load_thread_;
};


#endif
