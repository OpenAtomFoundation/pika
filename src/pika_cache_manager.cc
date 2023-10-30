// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/pika_cache_manager.h"

extern PikaServer* g_pika_server;
using CacheInfo = PikaCache::CacheInfo;

PikaCacheManager::PikaCacheManager() : cache_status_(PIKA_CACHE_STATUS_NONE) {
  cache::CacheConfig cache_config{};
  cache::RedisCache::SetConfig(&cache_config);
}

void PikaCacheManager::Init(const std::map<std::string, std::shared_ptr<DB>>& dbs) {
  std::shared_lock lg(mu_);
  for (const auto& kv : dbs) {
    auto db = kv.second;
    for (uint32_t i = 0; i < db->SlotNum(); ++i) {
      auto key = db->GetDBName() + std::to_string(i);
      caches_[key] = db->GetSlotById(i)->cache();
    }
  }
}

void PikaCacheManager::ProcessCronTask() {
  for (auto& cache : caches_) {
    cache.second->ActiveExpireCycle();
  }
  LOG(INFO) << "hit rate:" << HitRatio() << std::endl;
}

double PikaCacheManager::HitRatio(void) {
  std::unique_lock l(mu_);
  int64_t hits = 0;
  int64_t misses = 0;
  cache::RedisCache::GetHitAndMissNum(&hits, &misses);
  int64_t all_cmds = hits + misses;
  if (0 >= all_cmds) {
    return 0;
  }
  return hits / (all_cmds * 1.0);
}

void PikaCacheManager::ClearHitRatio(void) {
  std::unique_lock l(mu_);
  cache::RedisCache::ResetHitAndMissNum();
}

CacheInfo PikaCacheManager::Info() {
  CacheInfo info;
  std::unique_lock l(mu_);
  for (const auto &cache : caches_) {
    auto each_info = cache.second->Info();
    info.keys_num += each_info.keys_num;
    info.async_load_keys_num += each_info.async_load_keys_num;
  }
  info.used_memory = cache::RedisCache::GetUsedMemory();
  info.cache_num = caches_.size();
  cache::RedisCache::GetHitAndMissNum(&info.hits, &info.misses);
 return info;
}
