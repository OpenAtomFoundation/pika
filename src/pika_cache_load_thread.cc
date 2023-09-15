#include <glog/logging.h>

#include "include/pika_cache_load_thread.h"
#include "include/pika_server.h"
#include "include/pika_cache.h"
#include "pstd/include/scope_record_lock.h"
#include "include/pika_cache.h"

extern PikaServer *g_pika_server;

PikaCacheLoadThread::PikaCacheLoadThread(int cache_start_pos, int cache_items_per_key, std::shared_ptr<PikaCache> cache)
    : should_exit_(false),
      async_load_keys_num_(0),
      waitting_load_keys_num_(0),
      cache_start_pos_(cache_start_pos),
      cache_items_per_key_(cache_items_per_key),
      cache_(cache) {
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

void PikaCacheLoadThread::Push(const char key_type, std::string &key) {
  std::lock_guard lq(loadkeys_mutex_);
  std::lock_guard lm(loadkeys_map_mutex_);

  if (CACHE_LOAD_QUEUE_MAX_SIZE < loadkeys_queue_.size()) {
    // 5s打印一次日志
    static uint64_t last_log_time_us = 0;
    if (pstd::NowMicros() - last_log_time_us > 5000000) {
      LOG(WARNING) << "PikaCacheLoadThread::Push waiting...";
      last_log_time_us = pstd::NowMicros();
    }
    return;
  }

  if (loadkeys_map_.find(key) == loadkeys_map_.end()) {
    std::pair<const char, std::string> kpair = std::make_pair(key_type, key);
    loadkeys_queue_.push_back(kpair);
    loadkeys_map_[key] = std::string("");
    loadkeys_cond_.notify_all();
  }
}

bool PikaCacheLoadThread::LoadKv(std::string &key, const std::shared_ptr<Slot>& slot) {
  std::string value;
  int64_t ttl;
  slot->DbRWLockReader();
  rocksdb::Status s = slot->db()->GetWithTTL(key, &value, &ttl);
  slot->DbRWUnLock();
  if (!s.ok()) {
    LOG(WARNING) << "load kv failed, key=" << key;
    return false;
  }

  cache_->WriteKvToCache(key, value, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadHash(std::string &key, const std::shared_ptr<Slot>& slot) {
  int32_t len = 0;
  slot->DbRWLockReader();
  slot->db()->HLen(key, &len);
  slot->DbRWUnLock();
  if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    LOG(WARNING) << "can not load key, because item size:" << len
                 << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
    return false;
  }

  std::vector<storage::FieldValue> fvs;
  int64_t ttl;
  slot->DbRWLockReader();
  rocksdb::Status s = slot->db()->HGetallWithTTL(key, &fvs, &ttl);
  slot->DbRWUnLock();
  if (!s.ok()) {
    LOG(WARNING) << "load hash failed, key=" << key;
    return false;
  }

  cache_->WriteHashToCache(key, fvs, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadList(std::string &key, const std::shared_ptr<Slot>& slot) {
  uint64_t len = 0;
  slot->DbRWLockReader();
  slot->db()->LLen(key, &len);
  slot->DbRWLockWriter();
  if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    LOG(WARNING) << "can not load key, because item size:" << len
                 << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
    return false;
  }

  std::vector<std::string> values;
  int64_t ttl;
  slot->DbRWLockReader();
  rocksdb::Status s = slot->db()->LRangeWithTTL(key, 0, -1, &values, &ttl);
  slot->DbRWLockWriter();
  if (!s.ok()) {
    LOG(WARNING) << "load list failed, key=" << key;
    return false;
  }

  cache_->WriteListToCache(key, values, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadSet(std::string &key, const std::shared_ptr<Slot>& slot) {
  int32_t len = 0;
  slot->DbRWLockReader();
  slot->db()->SCard(key, &len);
  slot->DbRWLockWriter();
  if (0 >= len || CACHE_VALUE_ITEM_MAX_SIZE < len) {
    LOG(WARNING) << "can not load key, because item size:" << len
                 << " beyond max item size:" << CACHE_VALUE_ITEM_MAX_SIZE;
    return false;
  }

  std::vector<std::string> values;
  int64_t ttl;
  slot->DbRWLockReader();
  rocksdb::Status s = slot->db()->SMembersWithTTL(key, &values, &ttl);
  slot->DbRWLockWriter();
  if (!s.ok()) {
    LOG(WARNING) << "load set failed, key=" << key;
    return false;
  }

  cache_->WriteSetToCache(key, values, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadZset(std::string &key, const std::shared_ptr<Slot>& slot) {
  int32_t len = 0;
  int start_index = 0;
  int stop_index = -1;
  slot->DbRWLockReader();
  slot->db()->ZCard(key, &len);
  slot->DbRWUnLock();
  if (0 >= len) {
    return false;
  }

  unsigned long cache_len = 0;
  cache_->CacheZCard(key, &cache_len);
  if (cache_len != 0) {
    return true;
  }
  if (cache_start_pos_ == dory::CACHE_START_FROM_BEGIN) {
    if (cache_items_per_key_ <= len) {
      stop_index = cache_items_per_key_ - 1;
    }
  } else if (cache_start_pos_ == dory::CACHE_START_FROM_END) {
    if (cache_items_per_key_ <= len) {
      start_index = len - cache_items_per_key_;
    }
  }

  std::vector<storage::ScoreMember> score_members;
  int64_t ttl;
  slot->DbRWLockReader();
  rocksdb::Status s = slot->db()->ZRangeWithTTL(key, start_index, stop_index, &score_members, &ttl);
  slot->DbRWUnLock();
  if (!s.ok()) {
    LOG(WARNING) << "load zset failed, key=" << key;
    return false;
  }

  cache_->WriteZSetToCache(key, score_members, ttl);
  return true;
}

bool PikaCacheLoadThread::LoadKey(const char key_type, std::string &key, const std::shared_ptr<Slot>& slot) {
  // 加载缓存时，保证操作rocksdb和cache是原子的
  pstd::lock::MultiRecordLock record_lock(slot->LockMgr());

  switch (key_type) {
    case 'k':
      return LoadKv(key, slot);
    case 'h':
      return LoadHash(key, slot);
    case 'l':
      return LoadList(key, slot);
    case 's':
      return LoadSet(key, slot);
    case 'z':
      return LoadZset(key, slot);
    default:
      LOG(WARNING) << "PikaCacheLoadThread::LoadKey invalid key type : " << key_type;
      return false;
  }
}

void *PikaCacheLoadThread::ThreadMain() {
  LOG(INFO) << "PikaCacheLoadThread::ThreadMain Start";

  while (!should_exit_) {
    std::deque<std::pair<const char, std::string>> load_keys;
    {
      std::lock_guard lq(loadkeys_mutex_);
      waitting_load_keys_num_ = loadkeys_queue_.size();
      while (!should_exit_ && 0 >= loadkeys_queue_.size()) {
        loadkeys_cond_.notify_one();
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
    auto slot = cache_->GetSlot();
    for (auto iter = load_keys.begin(); iter != load_keys.end(); ++iter) {
      if (LoadKey(iter->first, iter->second, slot)) {
        ++async_load_keys_num_;
      } else {
        LOG(WARNING) << "PikaCacheLoadThread::ThreadMain LoadKey: " << iter->second << " failed !!!";
      }

      std::lock_guard lq(loadkeys_map_mutex_);
      loadkeys_map_.erase(iter->second);
    }
  }

  return nullptr;
}