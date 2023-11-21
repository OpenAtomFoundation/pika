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
#include "include/pika_slot.h"
#include "include/pika_define.h"
#include "net/include/net_thread.h"
#include "storage/storage.h"

#define CACHE_LOAD_QUEUE_MAX_SIZE 2048
#define CACHE_VALUE_ITEM_MAX_SIZE 2048
#define CACHE_LOAD_NUM_ONE_TIME 256

class PikaCacheLoadThread : public net::Thread {
 public:
  PikaCacheLoadThread(int cache_start_pos, int cache_items_per_key);
  ~PikaCacheLoadThread();

  uint64_t AsyncLoadKeysNum(void) { return async_load_keys_num_; }
  uint32_t WaittingLoadKeysNum(void) { return waitting_load_keys_num_; }
  void Push(const char key_type, std::string& key, const std::shared_ptr<Slot> &slot);

 private:
  bool LoadKV(std::string& key, const std::shared_ptr<Slot>& slot);
  bool LoadHash(std::string& key, const std::shared_ptr<Slot>& slot);
  bool LoadList(std::string& key, const std::shared_ptr<Slot>& slot);
  bool LoadSet(std::string& key, const std::shared_ptr<Slot>& slot);
  bool LoadZset(std::string& key, const std::shared_ptr<Slot>& slot);
  bool LoadKey(const char key_type, std::string& key, const std::shared_ptr<Slot>& slot);
  virtual void* ThreadMain();

 private:
  std::atomic<bool> should_exit_;
  std::deque<std::tuple<const char, std::string, const std::shared_ptr<Slot>>> loadkeys_queue_;
  pstd::CondVar loadkeys_cond_;
  pstd::Mutex loadkeys_mutex_;

  std::unordered_map<std::string, std::string> loadkeys_map_;
  pstd::Mutex loadkeys_map_mutex_;

  std::atomic<uint64_t> async_load_keys_num_;
  std::atomic<uint32_t> waitting_load_keys_num_;
  // currently only take effects to zset
  int cache_start_pos_;
  int cache_items_per_key_;
  std::shared_ptr<PikaCache> cache_;
};

#endif  // PIKA_CACHE_LOAD_THREAD_H_
