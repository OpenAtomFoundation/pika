// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#ifndef PIKA_CACHE_LOAD_THREAD_H_
#define PIKA_CACHE_LOAD_THREAD_H_

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/pika_cache.h"
#include "include/pika_define.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"

class PikaCacheLoadThread : public net::Thread {
 public:
  PikaCacheLoadThread(int zset_cache_start_direction, int zset_cache_field_num_per_key);
  ~PikaCacheLoadThread() override;

  uint64_t AsyncLoadKeysNum(void) { return async_load_keys_num_; }
  uint32_t WaittingLoadKeysNum(void) { return waitting_load_keys_num_; }
  void Push(const char key_type, std::string& key, const std::shared_ptr<DB>& db);

 private:
  bool LoadKV(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadHash(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadList(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadSet(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadZset(std::string& key, const std::shared_ptr<DB>& db);
  bool LoadKey(const char key_type, std::string& key, const std::shared_ptr<DB>& db);
  virtual void* ThreadMain() override;

 private:
  std::atomic_bool should_exit_;
  std::deque<std::tuple<const char, std::string, const std::shared_ptr<DB>>> loadkeys_queue_;
  
  pstd::CondVar loadkeys_cond_;
  pstd::Mutex loadkeys_mutex_;

  std::unordered_map<std::string, std::string> loadkeys_map_;
  pstd::Mutex loadkeys_map_mutex_;
  std::atomic_uint64_t async_load_keys_num_;
  std::atomic_uint32_t waitting_load_keys_num_;
  // currently only take effects to zset
  int zset_cache_start_direction_;
  int zset_cache_field_num_per_key_;
  std::shared_ptr<PikaCache> cache_;
};

#endif  // PIKA_CACHE_LOAD_THREAD_H_
