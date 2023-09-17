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
  std::shared_ptr<PikaCache> GetCache(const std::string& db_name, int slot_index);
  void Init(const std::vector<DBStruct>& dbs);
  void ProcessCronTask();
  void FlushDB(const std::string& db_name);
  void FlushAll();
  double HitRatio();
  void ClearHitRatio();
  PikaCache::CacheInfo Info();
 private:
  std::shared_mutex mu_;
  std::unordered_map<std::string, std::shared_ptr<PikaCache>> caches_;
  std::atomic<int> cache_status_;
  PikaCacheLoadThread *cache_load_thread_;
};


#endif
