// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "include/pika_cache_manager.h"

extern PikaServer* g_pika_server;

PikaCacheManager::PikaCacheManager() : cache_status_(PIKA_CACHE_STATUS_NONE) {
  dory::CacheConfig cacge_config{};
  dory::RedisCache::SetConfig(&cacge_config);
}

void PikaCacheManager::Init(std::vector<DBStruct> dbs) {
  std::shared_lock lg(mu_);
  for (const auto& db : dbs) {
    for (uint32_t i = 0; i < db.slot_num; ++i) {
      auto key = db.db_name + std::to_string(i);
      caches_.emplace(key, std::make_shared<PikaCache>(0, 0, g_pika_server->GetDBSlotById(db.db_name, i)));
      caches_[key]->Init();
    }
  }
}

PikaCacheManager::~PikaCacheManager() {
}

std::shared_ptr<PikaCache> PikaCacheManager::GetCache(const std::string& db_name, int slot_index) {
  std::shared_lock lg(mu_);
  auto key = db_name + std::to_string(slot_index);
  if (caches_.count(key) == 0) {
    return nullptr;
  }
  return caches_[key];
}

void PikaCacheManager::ProcessCronTask() {
  std::unique_lock lg(mu_);
  for (auto& cache : caches_) {
    cache.second->ActiveExpireCycle();
  }
}
void PikaCacheManager::FlushDB(const std::string& db_name) {
  std::unique_lock lg(mu_);
  for (auto& cache : caches_) {
    if (cache.first.compare(0, db_name.size(), db_name) == 0) {
      cache.second->FlushSlot();
    }
  }
}

void PikaCacheManager::FlushAll() {
  std::unique_lock lg(mu_);
  for (auto& cache : caches_) {
    cache.second->FlushSlot();
  }
}

double PikaCacheManager::HitRatio(void) {
  std::unique_lock l(mu_);
  long long hits = 0;
  long long misses = 0;
  dory::RedisCache::GetHitAndMissNum(&hits, &misses);
  long long all_cmds = hits + misses;
  if (0 >= all_cmds) {
    return 0;
  }
  return hits / (all_cmds * 1.0);
}

void PikaCacheManager::ClearHitRatio(void) {
  std::unique_lock l(mu_);
  dory::RedisCache::ResetHitAndMissNum();
}
