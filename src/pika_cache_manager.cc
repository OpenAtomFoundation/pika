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

void PikaCacheManager::ClearHitRatio(void) {
  std::unique_lock l(mu_);
  cache::RedisCache::ResetHitAndMissNum();
}

void PikaCacheManager::UpdateCacheInfo(void) {
  std::shared_ptr<Slot> slot;
  slot = g_pika_server->GetSlotByDBName(g_pika_conf->default_db());
  if (PIKA_CACHE_STATUS_OK != slot->cache()->CacheStatus()) {
     return;
  }

  // get cache info from redis cache
  PikaCache::CacheInfo cache_info;
  slot->cache()->Info(cache_info);
  std::unique_lock<std::shared_mutex> lock(cache_info_rwlock_);
  cache_info_.status = cache_info.status;
  cache_info_.cache_num = cache_info.cache_num;
  cache_info_.keys_num = cache_info.keys_num;
  cache_info_.used_memory = cache_info.used_memory;
  cache_info_.waitting_load_keys_num = cache_info.waitting_load_keys_num;
  cache_usage_ = cache_info.used_memory;

  uint64_t all_cmds = cache_info.hits + cache_info.misses;
  cache_info_.hitratio_all = (0 >= all_cmds) ? 0.0 : (cache_info.hits * 100.0) / all_cmds;

  uint64_t cur_time_us = pstd::NowMicros();
  uint64_t delta_time = cur_time_us - cache_info_.last_time_us + 1;
  uint64_t delta_hits = cache_info.hits - cache_info_.hits;
  cache_info_.hits_per_sec = delta_hits * 1000000 / delta_time;

  uint64_t delta_all_cmds = all_cmds - (cache_info_.hits + cache_info_.misses);
  cache_info_.read_cmd_per_sec = delta_all_cmds * 1000000 / delta_time;

  cache_info_.hitratio_per_sec = (0 >= delta_all_cmds) ? 0.0 : (delta_hits * 100.0) / delta_all_cmds;

  uint64_t delta_load_keys = cache_info.async_load_keys_num - cache_info_.last_load_keys_num;
  cache_info_.load_keys_per_sec = delta_load_keys * 1000000 / delta_time;

  cache_info_.hits = cache_info.hits;
  cache_info_.misses = cache_info.misses;
  cache_info_.last_time_us = cur_time_us;
  cache_info_.last_load_keys_num = cache_info.async_load_keys_num;
}

void PikaCacheManager::ResetDisplayCacheInfo(int status) {
  std::unique_lock<std::shared_mutex> lock(cache_info_rwlock_);
  cache_info_.status = status;
  cache_info_.cache_num = 0;
  cache_info_.keys_num = 0;
  cache_info_.used_memory = 0;
  cache_info_.hits = 0;
  cache_info_.misses = 0;
  cache_info_.hits_per_sec = 0;
  cache_info_.read_cmd_per_sec = 0;
  cache_info_.hitratio_per_sec = 0.0;
  cache_info_.hitratio_all = 0.0;
  cache_info_.load_keys_per_sec = 0;
  cache_info_.waitting_load_keys_num = 0;
  cache_usage_ = 0;
}

void PikaCacheManager::GetCacheInfo(DisplayCacheInfo &cache_info) {
  std::shared_lock<std::shared_mutex> lock(cache_info_rwlock_);
  cache_info = cache_info_;
}

void PikaCacheManager::CacheConfigInit(cache::CacheConfig& cache_cfg) {
  cache_cfg.maxmemory = g_pika_conf->cache_maxmemory();
  cache_cfg.maxmemory_policy = g_pika_conf->cache_maxmemory_policy();
  cache_cfg.maxmemory_samples = g_pika_conf->cache_maxmemory_samples();
  cache_cfg.lfu_decay_time = g_pika_conf->cache_lfu_decay_time();
}