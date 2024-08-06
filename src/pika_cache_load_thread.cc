// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <glog/logging.h>

#include "include/pika_cache_load_thread.h"
#include "include/pika_server.h"
#include "include/pika_cache.h"
#include "pstd/include/scope_record_lock.h"

extern PikaServer* g_pika_server;

PikaCacheLoadThread::PikaCacheLoadThread(int zset_cache_start_direction, int zset_cache_field_num_per_key)
    : should_exit_(false)
      , loadkeys_cond_()
      , async_load_keys_num_(0)
      , waitting_load_keys_num_(0)
      , zset_cache_start_direction_(zset_cache_start_direction)
      , zset_cache_field_num_per_key_(zset_cache_field_num_per_key)
{
  set_thread_name("PikaCacheLoadThread");
}

PikaCacheLoadThread::~PikaCacheLoadThread() {
  {
    std::lock_guard lq(loadkeys_mutex_);
    should_exit_ = true;
    loadkeys_cond_.notify_all();
  }

  StopThread();
}

void PikaCacheLoadThread::Push(const char key_type, std::string& key, const std::shared_ptr<DB>& db) {
  std::unique_lock lq(loadkeys_mutex_);
  std::unique_lock lm(loadkeys_map_mutex_);
  if (CACHE_LOAD_QUEUE_MAX_SIZE < loadkeys_queue_.size()) {
    // 5s to print logs once
    static uint64_t last_log_time_us = 0;
    if (pstd::NowMicros() - last_log_time_us > 5000000) {
      LOG(WARNING) << "PikaCacheLoadThread::Push waiting...";
      last_log_time_us = pstd::NowMicros();
    }
    return;
  }

  if (loadkeys_map_.find(key) == loadkeys_map_.end()) {
    std::tuple<const char, std::string, const std::shared_ptr<DB>> ktuple = std::make_tuple(key_type, key, db);
    loadkeys_queue_.push_back(ktuple);
    loadkeys_map_[key] = std::string("");
    loadkeys_cond_.notify_all();
  }
}

bool PikaCacheLoadThread::LoadKV(std::string& key, const std::shared_ptr<DB>& db) {
  std::string value;
  int64_t ttl = -1;
  rocksdb::Status s = db->storage()->GetWithTTL(key, &value, &ttl);
  if (!s.ok()) {
    LOG(WARNING) << "load kv failed, key=" << key;
    return false;
  }
  db->cache()->WriteKVToCache(key, value, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadHash(std::string& key, const std::shared_ptr<DB>& db) {
  int32_t len = 0;
  db->storage()->HLen(key, &len);
  if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    return false;
  }

  std::vector<storage::FieldValue> fvs;
  int64_t ttl = -1;
  rocksdb::Status s = db->storage()->HGetallWithTTL(key, &fvs, &ttl);
  if (!s.ok()) {
    LOG(WARNING) << "load hash failed, key=" << key;
    return false;
  }
  db->cache()->WriteHashToCache(key, fvs, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadList(std::string& key, const std::shared_ptr<DB>& db) {
  uint64_t len = 0;
  db->storage()->LLen(key, &len);
  if (len <= 0 || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    LOG(WARNING) << "can not load key, because item size:" << len
                 << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
    return false;
  }

  std::vector<std::string> values;
  int64_t ttl = -1;
  rocksdb::Status s = db->storage()->LRangeWithTTL(key, 0, -1, &values, &ttl);
  if (!s.ok()) {
    LOG(WARNING) << "load list failed, key=" << key;
    return false;
  }
  db->cache()->WriteListToCache(key, values, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadSet(std::string& key, const std::shared_ptr<DB>& db) {
  int32_t len = 0;
  db->storage()->SCard(key, &len);
  if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    LOG(WARNING) << "can not load key, because item size:" << len
                 << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
    return false;
  }

  std::vector<std::string> values;
  int64_t ttl_millsec = -1;
  rocksdb::Status s = db->storage()->SMembersWithTTL(key, &values, &ttl_millsec);
  if (!s.ok()) {
    LOG(WARNING) << "load set failed, key=" << key;
    return false;
  }
  db->cache()->WriteSetToCache(key, values, ttl_millsec > 0 ? ttl_millsec / 1000 : ttl_millsec);
  return true;
}

bool PikaCacheLoadThread::LoadZset(std::string& key, const std::shared_ptr<DB>& db) {
  int32_t len = 0;
  int start_index = 0;
  int stop_index = -1;
  db->storage()->ZCard(key, &len);
  if (0 >= len) {
    return false;
  }

  uint64_t cache_len = 0;
  db->cache()->CacheZCard(key, &cache_len);
  if (cache_len != 0) {
    return true;
  }
  if (zset_cache_start_direction_ == cache::CACHE_START_FROM_BEGIN) {
    if (zset_cache_field_num_per_key_ <= len) {
      stop_index = zset_cache_field_num_per_key_ - 1;
    }
  } else if (zset_cache_start_direction_ == cache::CACHE_START_FROM_END) {
    if (zset_cache_field_num_per_key_ <= len) {
      start_index = len - zset_cache_field_num_per_key_;
    }
  }

  std::vector<storage::ScoreMember> score_members;
  int64_t ttl = -1;
  rocksdb::Status s = db->storage()->ZRangeWithTTL(key, start_index, stop_index, &score_members, &ttl);
  if (!s.ok()) {
    LOG(WARNING) << "load zset failed, key=" << key;
    return false;
  }
  db->cache()->WriteZSetToCache(key, score_members, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadKey(const char key_type, std::string& key, const std::shared_ptr<DB>& db) {
  pstd::lock::ScopeRecordLock record_lock(db->LockMgr(), key);
  switch (key_type) {
    case 'k':
      return LoadKV(key, db);
    case 'h':
      return LoadHash(key, db);
    case 'l':
      return LoadList(key, db);
    case 's':
      return LoadSet(key, db);
    case 'z':
      return LoadZset(key, db);
    default:
      LOG(WARNING) << "PikaCacheLoadThread::LoadKey invalid key type : " << key_type;
      return false;
  }
}

void *PikaCacheLoadThread::ThreadMain() {
  LOG(INFO) << "PikaCacheLoadThread::ThreadMain Start";

  while (!should_exit_) {
    std::deque<std::tuple<const char, std::string, const std::shared_ptr<DB>>> load_keys;
    {
      std::unique_lock lq(loadkeys_mutex_);
      waitting_load_keys_num_ = loadkeys_queue_.size();
      while (!should_exit_ && loadkeys_queue_.size() <= 0) {
        loadkeys_cond_.wait(lq);
      }

      if (should_exit_) {
        return nullptr;
      }

      for (int i = 0; i < CACHE_LOAD_NUM_ONE_TIME; ++i) {
        if (!loadkeys_queue_.empty()) {
          load_keys.push_back(loadkeys_queue_.front());
          loadkeys_queue_.pop_front();
        }
      }
    }
    for (auto & load_key : load_keys) {
      if (LoadKey(std::get<0>(load_key), std::get<1>(load_key), std::get<2>(load_key))) {
        ++async_load_keys_num_;
      }

      std::unique_lock lm(loadkeys_map_mutex_);
      loadkeys_map_.erase(std::get<1>(load_key));
    }
  }

  return nullptr;
}
