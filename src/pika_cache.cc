// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <glog/logging.h>
#include <unordered_set>
#include <thread>

#include "include/pika_cache.h"
#include "include/pika_cache_load_thread.h"
#include "include/pika_server.h"
#include "include/pika_slot_command.h"
#include "cache/include/cache.h"
#include "cache/include/config.h"
#include "include/pika_define.h"

extern PikaServer *g_pika_server;
#define EXTEND_CACHE_SIZE(N) (N * 12 / 10)

using Status = rocksdb::Status;

PikaCache::PikaCache(int cache_start_pos, int cache_items_per_key, std::shared_ptr<Slot> slot)
    : cache_status_(PIKA_CACHE_STATUS_NONE),
      cache_start_pos_(cache_start_pos),
      cache_items_per_key_(EXTEND_CACHE_SIZE(cache_items_per_key)),
      slot_(slot) {
  cache_ = std::make_unique<cache::RedisCache>();
}

PikaCache::~PikaCache() {
  {
    std::unique_lock l(rwlock_);
    DestroyWithoutLock();
  }
}

Status PikaCache::Init() {
  std::unique_lock l(rwlock_);
  return InitWithoutLock();
}

Status PikaCache::Reset() {
  std::unique_lock l(rwlock_);
  DestroyWithoutLock();
  return InitWithoutLock();
}

void PikaCache::ResetConfig(cache::CacheConfig *cache_cfg) {
  std::unique_lock l(rwlock_);
  cache_start_pos_ = cache_cfg->cache_start_pos;
  cache_items_per_key_ = EXTEND_CACHE_SIZE(cache_cfg->cache_items_per_key);
  LOG(WARNING) << "cache_start_pos: " << cache_start_pos_ << ", cache_items_per_key: " << cache_items_per_key_;
  cache::RedisCache::SetConfig(cache_cfg);
}

void PikaCache::Destroy(void) {
  std::unique_lock l(rwlock_);
  DestroyWithoutLock();
}

void PikaCache::SetCacheStatus(int status) { cache_status_ = status; }

int PikaCache::CacheStatus(void) { return cache_status_; }

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
PikaCache::CacheInfo PikaCache::Info(CacheInfo &info) {
  std::unique_lock l(rwlock_);
  info.status = cache_status_;
  info.async_load_keys_num = cache_load_thread_->AsyncLoadKeysNum();
  info.waitting_load_keys_num = cache_load_thread_->WaittingLoadKeysNum();
  info.keys_num = cache_->DbSize();
}

bool PikaCache::Exists(std::string &key) {
  std::shared_lock l(rwlock_);
  return cache_->Exists(key);
}

void PikaCache::FlushSlot(void) {
  std::unique_lock l(rwlock_);
  cache_->FlushDb();
}

void PikaCache::ActiveExpireCycle() {
  std::unique_lock l(rwlock_);
  cache_->ActiveExpireCycle();
}

Status PikaCache::Del(const std::vector<std::string> &keys) {
  std::unique_lock l(rwlock_);
  for (const auto &key : keys) {
    cache_->Del(key);
  }
  return Status::OK();
}

Status PikaCache::Expire(std::string &key, int64_t ttl) {
  std::unique_lock l(rwlock_);
  return cache_->Expire(key, ttl);
}

Status PikaCache::Expireat(std::string &key, int64_t ttl) {
  std::unique_lock l(rwlock_);
  return cache_->Expireat(key, ttl);
}

Status PikaCache::TTL(std::string &key, int64_t *ttl) {
  std::unique_lock l(rwlock_);
  return cache_->TTL(key, ttl);
}

Status PikaCache::Persist(std::string &key) {
  std::unique_lock l(rwlock_);
  return cache_->Persist(key);
}

Status PikaCache::Type(std::string &key, std::string *value) {
  std::unique_lock l(rwlock_);
  return cache_->Type(key, value);
}

Status PikaCache::RandomKey(std::string *key) {
  std::unique_lock l(rwlock_);
  return cache_->RandomKey(key);
}

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::Set(std::string &key, std::string &value, int64_t ttl) {
  std::unique_lock l(rwlock_);
  return cache_->Set(key, value, ttl);
}
Status PikaCache::SetWithoutTTL(std::string &key, std::string &value) {
  std::unique_lock l(rwlock_);
  return cache_->SetWithoutTTL(key, value);
}

Status PikaCache::Setnx(std::string &key, std::string &value, int64_t ttl) {
  std::unique_lock l(rwlock_);
  return cache_->Setnx(key, value, ttl);
}

Status PikaCache::SetnxWithoutTTL(std::string &key, std::string &value) {
  std::unique_lock l(rwlock_);
  return cache_->SetnxWithoutTTL(key, value);
}

Status PikaCache::Setxx(std::string &key, std::string &value, int64_t ttl) {
  std::unique_lock l(rwlock_);
  return cache_->Setxx(key, value, ttl);
}

Status PikaCache::SetxxWithoutTTL(std::string &key, std::string &value) {
  std::unique_lock l(rwlock_);
  return cache_->SetxxWithoutTTL(key, value);
}
Status PikaCache::MSet(const std::vector<storage::KeyValue> &kvs) {
  std::unique_lock l(rwlock_);
  for (const auto &item : kvs) {
    auto [key, value] = item;
    cache_->SetWithoutTTL(key, value);
  }
  return Status::OK();
}

Status PikaCache::Get(std::string &key, std::string *value) {
  std::shared_lock l(rwlock_);
  return cache_->Get(key, value);
}

Status PikaCache::MGet(const std::vector<std::string> &keys, std::vector<storage::ValueStatus> *vss) {
  std::shared_lock l(rwlock_);
  vss->resize(keys.size());
  auto ret = Status::OK();
  for (int i = 0; i < keys.size(); ++i) {
    auto s = cache_->Get(keys[i], &(*vss)[i].value);
    (*vss)[i].status = s;
    if (!s.ok()) {
      ret = s;
    }
  }
  return ret;
}

Status PikaCache::Incrxx(std::string &key) {
  std::unique_lock l(rwlock_);
  if (cache_->Exists(key)) {
    return cache_->Incr(key);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::Decrxx(std::string &key) {
  std::unique_lock l(rwlock_);
  if (cache_->Exists(key)) {
    return cache_->Decr(key);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::IncrByxx(std::string &key, uint64_t incr) {
  std::unique_lock l(rwlock_);
  if (cache_->Exists(key)) {
    return cache_->IncrBy(key, incr);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::DecrByxx(std::string &key, uint64_t incr) {
  std::unique_lock l(rwlock_);

  if (cache_->Exists(key)) {
    return cache_->DecrBy(key, incr);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::Incrbyfloatxx(std::string &key, long double incr) {
  std::unique_lock l(rwlock_);
  
  if (cache_->Exists(key)) {
    return cache_->Incrbyfloat(key, incr);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::Appendxx(std::string &key, std::string &value) {
  std::unique_lock l(rwlock_);

  if (cache_->Exists(key)) {
    return cache_->Append(key, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::GetRange(std::string &key, int64_t start, int64_t end, std::string *value) {
  std::unique_lock l(rwlock_);

  return cache_->GetRange(key, start, end, value);
}

Status PikaCache::SetRangexx(std::string &key, int64_t start, std::string &value) {
  std::unique_lock l(rwlock_);

  if (cache_->Exists(key)) {
    return cache_->SetRange(key, start, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::Strlen(std::string &key, int32_t *len) {
  std::unique_lock l(rwlock_);
  return cache_->Strlen(key, len);
}

/*-----------------------------------------------------------------------------
 * Hash Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::HDel(std::string &key, std::vector<std::string> &fields) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HDel(key, fields);
}

Status PikaCache::HSet(std::string &key, std::string &field, std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HSet(key, field, value);
}

Status PikaCache::HSetIfKeyExist(std::string &key, std::string &field, std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->HSet(key, field, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::HSetIfKeyExistAndFieldNotExist(std::string &key, std::string &field, std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->HSetnx(key, field, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::HMSet(std::string &key, std::vector<storage::FieldValue> &fvs) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HMSet(key, fvs);
}

Status PikaCache::HMSetnx(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl) {
  std::unique_lock l(rwlock_);

  
  
  if (!cache_->Exists(key)) {
    cache_->HMSet(key, fvs);
    cache_->Expire(key, ttl);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::HMSetnxWithoutTTL(std::string &key, std::vector<storage::FieldValue> &fvs) {
  std::unique_lock l(rwlock_);

  
  
  if (!cache_->Exists(key)) {
    cache_->HMSet(key, fvs);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::HMSetxx(std::string &key, std::vector<storage::FieldValue> &fvs) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->HMSet(key, fvs);
  } else {
    return Status::NotFound("key not exist");
  }
}

Status PikaCache::HGet(std::string &key, std::string &field, std::string *value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HGet(key, field, value);
}

Status PikaCache::HMGet(std::string &key, std::vector<std::string> &fields, std::vector<storage::ValueStatus> *vss) {
  std::unique_lock l(rwlock_);
  return cache_->HMGet(key, fields, vss);
}

Status PikaCache::HGetall(std::string &key, std::vector<storage::FieldValue> *fvs) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HGetall(key, fvs);
}

Status PikaCache::HKeys(std::string &key, std::vector<std::string> *fields) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HKeys(key, fields);
}

Status PikaCache::HVals(std::string &key, std::vector<std::string> *values) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HVals(key, values);
}

Status PikaCache::HExists(std::string &key, std::string &field) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HExists(key, field);
}

Status PikaCache::HIncrbyxx(std::string &key, std::string &field, int64_t value) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->HIncrby(key, field, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::HIncrbyfloatxx(std::string &key, std::string &field, long double value) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->HIncrbyfloat(key, field, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::HLen(std::string &key, uint64_t *len) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->HLen(key, len);
}

Status PikaCache::HStrlen(std::string &key, std::string &field, uint64_t *len) {
  std::unique_lock l(rwlock_);
  return cache_->HStrlen(key, field, len);
}

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::LIndex(std::string &key, int64_t index, std::string *element) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LIndex(key, index, element);
}

Status PikaCache::LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot,
                          std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LInsert(key, before_or_after, pivot, value);
}

Status PikaCache::LLen(std::string &key, uint64_t *len) {
  std::unique_lock l(rwlock_);


  
  return cache_->LLen(key, len);
}

Status PikaCache::LPop(std::string &key, std::string *element) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LPop(key, element);
}

Status PikaCache::LPush(std::string &key, std::vector<std::string> &values) {
  std::unique_lock l(rwlock_);
  
  
  return cache_->LPush(key, values);
}

Status PikaCache::LPushx(std::string &key, std::vector<std::string> &values) {
  std::unique_lock l(rwlock_);
  return cache_->LPushx(key, values);
}

Status PikaCache::LRange(std::string &key, int64_t start, int64_t stop, std::vector<std::string> *values) {
  std::unique_lock l(rwlock_);
  return cache_->LRange(key, start, stop, values);
}

Status PikaCache::LRem(std::string &key, int64_t count, std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LRem(key, count, value);
}

Status PikaCache::LSet(std::string &key, int64_t index, std::string &value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LSet(key, index, value);
}

Status PikaCache::LTrim(std::string &key, int64_t start, int64_t stop) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->LTrim(key, start, stop);
}

Status PikaCache::RPop(std::string &key, std::string *element) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->RPop(key, element);
}

Status PikaCache::RPush(std::string &key, std::vector<std::string> &values) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->RPush(key, values);
}

Status PikaCache::RPushx(std::string &key, std::vector<std::string> &values) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->RPushx(key, values);
}

Status PikaCache::RPushnx(std::string &key, std::vector<std::string> &values, int64_t ttl) {
  std::unique_lock l(rwlock_);

  
  if (!cache_->Exists(key)) {
    cache_->RPush(key, values);
    cache_->Expire(key, ttl);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::RPushnxWithoutTTL(std::string &key, std::vector<std::string> &values) {
  std::unique_lock l(rwlock_);

  
  
  if (!cache_->Exists(key)) {
    cache_->RPush(key, values);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::SAdd(std::string &key, std::vector<std::string> &members) {
  std::unique_lock l(rwlock_);
  return cache_->SAdd(key, members);
}

Status PikaCache::SAddIfKeyExist(std::string &key, std::vector<std::string> &members) {
  std::unique_lock l(rwlock_);

  if (cache_->Exists(key)) {
    return cache_->SAdd(key, members);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::SAddnx(std::string &key, std::vector<std::string> &members, int64_t ttl) {
  std::unique_lock l(rwlock_);

  
  
  if (!cache_->Exists(key)) {
    cache_->SAdd(key, members);
    cache_->Expire(key, ttl);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::SAddnxWithoutTTL(std::string &key, std::vector<std::string> &members) {
  std::unique_lock l(rwlock_);
  if (!cache_->Exists(key)) {
    cache_->SAdd(key, members);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::SCard(std::string &key, uint64_t *len) {
  std::unique_lock l(rwlock_);
  return cache_->SCard(key, len);
}

Status PikaCache::SIsmember(std::string &key, std::string &member) {
  std::unique_lock l(rwlock_);
  return cache_->SIsmember(key, member);
}

Status PikaCache::SMembers(std::string &key, std::vector<std::string> *members) {
  std::unique_lock l(rwlock_);
  return cache_->SMembers(key, members);
}

Status PikaCache::SRem(std::string &key, std::vector<std::string> &members) {
  std::unique_lock l(rwlock_);
  return cache_->SRem(key, members);
}

Status PikaCache::SRandmember(std::string &key, int64_t count, std::vector<std::string> *members) {
  std::unique_lock l(rwlock_);
  return cache_->SRandmember(key, count, members);
}

/*-----------------------------------------------------------------------------
 * ZSet Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->ZAdd(key, score_members);
}

void PikaCache::GetMinMaxScore(std::vector<storage::ScoreMember> &score_members, double &min, double &max) {
  if (score_members.empty()) {
    return;
  }
  min = max = score_members.front().score;
  for (auto &item : score_members) {
    if (item.score < min) {
      min = item.score;
    }
    if (item.score > max) {
      max = item.score;
    }
  }
}

bool PikaCache::GetCacheMinMaxSM(cache::RedisCache *cache_obj, std::string &key, storage::ScoreMember &min_m,
                                 storage::ScoreMember &max_m) {
  if (cache_obj) {
    std::vector<storage::ScoreMember> score_members;
    auto s = cache_obj->ZRange(key, 0, 0, &score_members);
    if (!s.ok() || score_members.empty()) {
      return false;
    }
    min_m = score_members.front();
    score_members.clear();

    s = cache_obj->ZRange(key, -1, -1, &score_members);
    if (!s.ok() || score_members.empty()) {
      return false;
    }
    max_m = score_members.front();
    return true;
  }
  return false;
}

Status PikaCache::ZAddIfKeyExist(std::string &key, std::vector<storage::ScoreMember> &score_members) {
  // 看下为什么需要使用unique，如果没有必须要求，没必要使用unique
  std::unique_lock l(rwlock_);

  auto cache_obj = cache_.get();
  Status s;
  if (cache_obj->Exists(key)) {
    std::unordered_set<std::string> unique;
    std::list<storage::ScoreMember> filtered_score_members;
    for (auto it = score_members.rbegin(); it != score_members.rend(); ++it) {
      if (unique.find(it->member) == unique.end()) {
        unique.insert(it->member);
        filtered_score_members.push_front(*it);
      }
    }
    std::vector<storage::ScoreMember> new_score_members;
    for (auto &item : filtered_score_members) {
      new_score_members.push_back(std::move(item));
    }

    double min_score = storage::ZSET_SCORE_MIN;
    double max_score = storage::ZSET_SCORE_MAX;
    GetMinMaxScore(new_score_members, min_score, max_score);

    storage::ScoreMember cache_min_sm;
    storage::ScoreMember cache_max_sm;
    if (!GetCacheMinMaxSM(cache_obj, key, cache_min_sm, cache_max_sm)) {
      return Status::NotFound("key not exist");
    }
    auto cache_min_score = cache_min_sm.score;
    auto cache_max_score = cache_max_sm.score;
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      if (max_score < cache_max_score) {
        cache_obj->ZAdd(key, new_score_members);
      } else {
        std::vector<storage::ScoreMember> score_members_can_add;
        std::vector<std::string> members_need_remove;
        bool left_close = false;
        for (auto &item : new_score_members) {
          if (item.score == cache_max_score) {
            left_close = true;
            score_members_can_add.push_back(item);
            continue;
          }
          if (item.score < cache_max_score) {
            score_members_can_add.push_back(item);
          } else {
            members_need_remove.push_back(item.member);
          }
        }
        if (!score_members_can_add.empty()) {
          cache_obj->ZAdd(key, score_members_can_add);
          std::string cache_max_score_str = left_close ? "" : "(" + std::to_string(cache_max_score);
          std::string max_str = "+inf";
          cache_obj->ZRemrangebyscore(key, cache_max_score_str, max_str);
        }
        if (!members_need_remove.empty()) {
          cache_obj->ZRem(key, members_need_remove);
        }
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      if (min_score > cache_min_score) {
        cache_obj->ZAdd(key, new_score_members);
      } else {
        std::vector<storage::ScoreMember> score_members_can_add;
        std::vector<std::string> members_need_remove;
        bool right_close = false;
        for (auto &item : new_score_members) {
          if (item.score == cache_min_score) {
            right_close = true;
            score_members_can_add.push_back(item);
            continue;
          }
          if (item.score > cache_min_score) {
            score_members_can_add.push_back(item);
          } else {
            members_need_remove.push_back(item.member);
          }
        }
        if (!score_members_can_add.empty()) {
          cache_obj->ZAdd(key, score_members_can_add);
          std::string cache_min_score_str = right_close ? "" : "(" + std::to_string(cache_min_score);
          std::string min_str = "-inf";
          cache_obj->ZRemrangebyscore(key, min_str, cache_min_score_str);
        }
        if (!members_need_remove.empty()) {
          cache_obj->ZRem(key, members_need_remove);
        }
      }
    }

    return CleanCacheKeyIfNeeded(cache_obj, key);
  } else {
    return Status::NotFound("key not exist");
  }
}

Status PikaCache::CleanCacheKeyIfNeeded(cache::RedisCache *cache_obj, std::string &key) {
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len > (unsigned long)cache_items_per_key_) {
    long start = 0;
    long stop = 0;
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      // 淘汰尾部
      start = -cache_len + cache_items_per_key_;
      stop = -1;
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      // 淘汰头部
      start = 0;
      stop = cache_len - cache_items_per_key_ - 1;
    }
    auto min = std::to_string(start);
    auto max = std::to_string(stop);
    cache_obj->ZRemrangebyrank(key, min, max);
  }
  return Status::OK();
}

Status PikaCache::ZAddnx(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl) {
  std::unique_lock l(rwlock_);

  
  
  if (!cache_->Exists(key)) {
    cache_->ZAdd(key, score_members);
    cache_->Expire(key, ttl);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::ZAddnxWithoutTTL(std::string &key, std::vector<storage::ScoreMember> &score_members) {
  std::unique_lock l(rwlock_);

  
  if (!cache_->Exists(key)) {
    cache_->ZAdd(key, score_members);
    return Status::OK();
  } else {
    return Status::NotFound("key exist");
  }
}

Status PikaCache::ZCard(std::string &key, uint32_t *len, const std::shared_ptr<Slot> &slot) {
  int32_t db_len = 0;
  slot->db()->ZCard(key, &db_len);
  *len = db_len;
  return Status::OK();
}

Status PikaCache::CacheZCard(std::string &key, uint64_t *len) {
  std::unique_lock l(rwlock_);
  return cache_->ZCard(key, len);
}

RangeStatus PikaCache::CheckCacheRangeByScore(unsigned long cache_len, double cache_min, double cache_max, double min,
                                              double max, bool left_close, bool right_close) {
  bool cache_full = (cache_len == (unsigned long)cache_items_per_key_);

  if (cache_full) {
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      bool ret = (max < cache_max);
      if (ret) {
        if (max < cache_min) {
          return RangeStatus::RangeError;
        } else {
          return RangeStatus::RangeHit;
        }
      } else {
        return RangeStatus::RangeMiss;
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      bool ret = min > cache_min;
      if (ret) {
        if (min > cache_max) {
          return RangeStatus::RangeError;
        } else {
          return RangeStatus::RangeHit;
        }
      } else {
        return RangeStatus::RangeMiss;
      }
    } else {
      return RangeStatus::RangeError;
    }
  } else {
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      bool ret = right_close ? max < cache_max : max <= cache_max;
      if (ret) {
        if (max < cache_min) {
          return RangeStatus::RangeError;
        } else {
          return RangeStatus::RangeHit;
        }
      } else {
        return RangeStatus::RangeMiss;
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      bool ret = left_close ? min > cache_min : min >= cache_min;
      if (ret) {
        if (min > cache_max) {
          return RangeStatus::RangeError;
        } else {
          return RangeStatus::RangeHit;
        }
      } else {
        return RangeStatus::RangeMiss;
      }
    } else {
      return RangeStatus::RangeError;
    }
  }
}

Status PikaCache::ZCount(std::string &key, std::string &min, std::string &max, uint64_t *len, ZCountCmd *cmd) {
  std::unique_lock l(rwlock_);

  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    storage::ScoreMember cache_min_sm;
    storage::ScoreMember cache_max_sm;
    if (!GetCacheMinMaxSM(cache_obj, key, cache_min_sm, cache_max_sm)) {
      return Status::NotFound("key not exist");
    }
    auto cache_min_score = cache_min_sm.score;
    auto cache_max_score = cache_max_sm.score;

    if (RangeStatus::RangeHit == CheckCacheRangeByScore(cache_len, cache_min_score, cache_max_score, cmd->MinScore(),
                                                        cmd->MaxScore(), cmd->LeftClose(), cmd->RightClose())) {
      auto s = cache_obj->ZCount(key, min, max, len);
      return s;
    } else {
      return Status::NotFound("key not in cache");
    }
  }
}

Status PikaCache::ZIncrby(std::string &key, std::string &member, double increment) {
  std::unique_lock l(rwlock_);
  return cache_->ZIncrby(key, member, increment);
}

bool PikaCache::ReloadCacheKeyIfNeeded(cache::RedisCache *cache_obj, std::string &key, int mem_len, int db_len,
                                       const std::shared_ptr<Slot> &slot) {
  if (mem_len == -1) {
    uint64_t cache_len = 0;
    cache_obj->ZCard(key, &cache_len);
    mem_len = cache_len;
  }
  if (db_len == -1) {
    db_len = 0;
    slot->db()->ZCard(key, &db_len);
    if (!db_len) {
      return false;
    }
  }
  if (db_len < cache_items_per_key_) {
    if (mem_len * 2 < db_len) {
      cache_obj->Del(key);
      PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key);
      return true;
    } else {
      return false;
    }
  } else {
    if (cache_items_per_key_ && mem_len * 2 < cache_items_per_key_) {
      cache_obj->Del(key);
      PushKeyToAsyncLoadQueue(PIKA_KEY_TYPE_ZSET, key);
      return true;
    } else {
      return false;
    }
  }
}

Status PikaCache::ZIncrbyIfKeyExist(std::string &key, std::string &member, double increment, ZIncrbyCmd *cmd) {
  auto eps = std::numeric_limits<double>::epsilon();
  if (-eps < increment && increment < eps) {
    return Status::NotFound("icrement is 0, nothing to be done");
  }
  if (!cmd->res().ok()) {
    return Status::NotFound("key not exist");
  }
  std::unique_lock l(rwlock_);

  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);

  storage::ScoreMember cache_min_sm;
  storage::ScoreMember cache_max_sm;
  if (!GetCacheMinMaxSM(cache_obj, key, cache_min_sm, cache_max_sm)) {
    return Status::NotFound("key not exist");
  }
  auto cache_min_score = cache_min_sm.score;
  auto cache_max_score = cache_max_sm.score;
  auto RemCacheRangebyscoreAndCheck = [this, cache_obj, &key, cache_len](double score) {
    auto score_rm = std::to_string(score);
    auto s = cache_obj->ZRemrangebyscore(key, score_rm, score_rm);
    ReloadCacheKeyIfNeeded(cache_obj, key, cache_len);
    return s;
  };
  auto RemCacheKeyMember = [this, cache_obj, &key, cache_len](const std::string &member, bool check = true) {
    std::vector<std::string> member_rm = {member};
    auto s = cache_obj->ZRem(key, member_rm);
    if (check) {
      ReloadCacheKeyIfNeeded(cache_obj, key, cache_len);
    }
    return s;
  };

  if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
    if (cmd->Score() > cache_max_score) {
      return RemCacheKeyMember(member);
    } else if (cmd->Score() == cache_max_score) {
      RemCacheKeyMember(member, false);
      return RemCacheRangebyscoreAndCheck(cache_max_score);
    } else {
      std::vector<storage::ScoreMember> score_member = {{cmd->Score(), member}};
      auto s = cache_obj->ZAdd(key, score_member);
      CleanCacheKeyIfNeeded(cache_obj, key);
      return s;
    }
  } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
    if (cmd->Score() > cache_min_score) {
      std::vector<storage::ScoreMember> score_member = {{cmd->Score(), member}};
      auto s = cache_obj->ZAdd(key, score_member);
      CleanCacheKeyIfNeeded(cache_obj, key);
      return s;
    } else if (cmd->Score() == cache_min_score) {
      RemCacheKeyMember(member, false);
      return RemCacheRangebyscoreAndCheck(cache_min_score);
    } else {
      std::vector<std::string> member_rm = {member};
      return RemCacheKeyMember(member);
    }
  }

  return Status::NotFound("key not exist");
}

RangeStatus PikaCache::CheckCacheRange(int32_t cache_len, int32_t db_len, int64_t start, int64_t stop, int64_t &out_start,
                                       int64_t &out_stop) {
  out_start = start >= 0 ? start : db_len + start;
  out_stop = stop >= 0 ? stop : db_len + stop;
  out_start = out_start <= 0 ? 0 : out_start;
  out_stop = out_stop >= db_len ? db_len - 1 : out_stop;
  if (out_start > out_stop || out_start >= db_len || out_stop < 0) {
    return RangeStatus::RangeError;
  } else {
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      if (out_start < cache_len && out_stop < cache_len) {
        return RangeStatus::RangeHit;
      } else {
        return RangeStatus::RangeMiss;
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      if (out_start >= db_len - cache_len && out_stop >= db_len - cache_len) {
        out_start = out_start - (db_len - cache_len);
        out_stop = out_stop - (db_len - cache_len);
        return RangeStatus::RangeHit;
      } else {
        return RangeStatus::RangeMiss;
      }
    } else {
      return RangeStatus::RangeError;
    }
  }
}

RangeStatus PikaCache::CheckCacheRevRange(int32_t cache_len, int32_t db_len, int64_t start, int64_t stop, int64_t &out_start,
                                          int64_t &out_stop) {
  // db 正向的index
  long start_index = stop >= 0 ? db_len - stop - 1 : -stop - 1;
  long stop_index = start >= 0 ? db_len - start - 1 : -start - 1;
  start_index = start_index <= 0 ? 0 : start_index;
  stop_index = stop_index >= db_len ? db_len - 1 : stop_index;
  if (start_index > stop_index || start_index >= db_len || stop_index < 0) {
    return RangeStatus::RangeError;
  } else {
    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      if (start_index < cache_len && stop_index < cache_len) {
        // cache 逆向的index
        out_start = cache_len - stop_index - 1;
        out_stop = cache_len - start_index - 1;

        return RangeStatus::RangeHit;
      } else {
        return RangeStatus::RangeMiss;
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      if (start_index >= db_len - cache_len && stop_index >= db_len - cache_len) {
        // cache 正向的index
        int cache_start = start_index - (db_len - cache_len);
        int cache_stop = stop_index - (db_len - cache_len);
        // cache 逆向的index
        out_start = cache_len - cache_stop - 1;
        out_stop = cache_len - cache_start - 1;
        return RangeStatus::RangeHit;
      } else {
        return RangeStatus::RangeMiss;
      }
    } else {
      return RangeStatus::RangeError;
    }
  }
}

Status PikaCache::ZRange(std::string &key, int64_t start, int64_t stop, std::vector<storage::ScoreMember> *score_members,
                         const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);

  
  

  auto cache_obj = cache_.get();
  auto db_obj = slot->db();
  Status s;
  if (cache_obj->Exists(key)) {
    uint64_t cache_len = 0;
    cache_obj->ZCard(key, &cache_len);
    int32_t db_len = 0;
    db_obj->ZCard(key, &db_len);
    int64_t out_start;
    int64_t out_stop;
    RangeStatus rs = CheckCacheRange(cache_len, db_len, start, stop, out_start, out_stop);
    if (rs == RangeStatus::RangeHit) {
      return cache_obj->ZRange(key, out_start, out_stop, score_members);
    } else if (rs == RangeStatus::RangeMiss) {
      ReloadCacheKeyIfNeeded(cache_obj, key, cache_len, db_len);
      return Status::NotFound("key not in cache");
    } else if (rs == RangeStatus::RangeError) {
      return Status::NotFound("error range");
    } else {
      return Status::Corruption("unknown error");
    }
  } else {
    return Status::NotFound("key not in cache");
  }
}

Status PikaCache::ZRangebyscore(std::string &key, std::string &min, std::string &max,
                                std::vector<storage::ScoreMember> *score_members, ZRangebyscoreCmd *cmd) {
  std::unique_lock l(rwlock_);

  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    storage::ScoreMember cache_min_sm;
    storage::ScoreMember cache_max_sm;
    if (!GetCacheMinMaxSM(cache_obj, key, cache_min_sm, cache_max_sm)) {
      return Status::NotFound("key not exist");
    }

    if (RangeStatus::RangeHit == CheckCacheRangeByScore(cache_len, cache_min_sm.score, cache_max_sm.score,
                                                        cmd->MinScore(), cmd->MaxScore(), cmd->LeftClose(),
                                                        cmd->RightClose())) {
      return cache_obj->ZRangebyscore(key, min, max, score_members, cmd->Offset(), cmd->Count());
    } else {
      return Status::NotFound("key not in cache");
    }
  }
}

Status PikaCache::ZRank(std::string &key, std::string &member, int64_t *rank, const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  
  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    auto s = cache_obj->ZRank(key, member, rank);
    if (s.ok()) {
      if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
        int32_t db_len = 0;
        slot->db()->ZCard(key, &db_len);
        *rank = db_len - cache_len + *rank;
      }
      return s;
    } else {
      return Status::NotFound("key not in cache");
    }
  }
}

Status PikaCache::ZRem(std::string &key, std::vector<std::string> &members, std::shared_ptr<Slot> slot) {
  std::unique_lock l(rwlock_);
  auto s = cache_->ZRem(key, members);
  ReloadCacheKeyIfNeeded(cache_.get(), key);
  return s;
}

Status PikaCache::ZRemrangebyrank(std::string &key, std::string &min, std::string &max, int32_t ele_deleted,
                                  const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    auto db_obj = slot->db();
    int32_t db_len = 0;
    db_obj->ZCard(key, &db_len);
    db_len += ele_deleted;
    auto start = std::stol(min);
    auto stop = std::stol(max);

    int32_t start_index = start >= 0 ? start : db_len + start;
    int32_t stop_index = stop >= 0 ? stop : db_len + stop;
    start_index = start_index <= 0 ? 0 : start_index;
    stop_index = stop_index >= db_len ? db_len - 1 : stop_index;
    if (start_index > stop_index) {
      return Status::NotFound("error range");
    }

    if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
      if ((uint32_t)start_index <= cache_len) {
        auto cache_min_str = std::to_string(start_index);
        auto cache_max_str = std::to_string(stop_index);
        auto s = cache_obj->ZRemrangebyrank(key, cache_min_str, cache_max_str);
        ReloadCacheKeyIfNeeded(cache_obj, key, cache_len, db_len - ele_deleted);
        return s;
      } else {
        return Status::NotFound("error range");
      }
    } else if (cache_start_pos_ == cache::CACHE_START_FROM_END) {
      if ((uint32_t)stop_index >= db_len - cache_len) {
        int32_t cache_min = start_index - (db_len - cache_len);
        int32_t cache_max = stop_index - (db_len - cache_len);
        cache_min = cache_min <= 0 ? 0 : cache_min;
        cache_max = cache_max >= (int32_t)cache_len ? cache_len - 1 : cache_max;

        auto cache_min_str = std::to_string(cache_min);
        auto cache_max_str = std::to_string(cache_max);
        auto s = cache_obj->ZRemrangebyrank(key, cache_min_str, cache_max_str);

        ReloadCacheKeyIfNeeded(cache_obj, key, cache_len, db_len - ele_deleted);
        return s;
      } else {
        return Status::NotFound("error range");
      }
    } else {
      return Status::NotFound("error range");
    }
  }
}

Status PikaCache::ZRemrangebyscore(std::string &key, std::string &min, std::string &max,
                                   const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  auto s = cache_->ZRemrangebyscore(key, min, max);
  ReloadCacheKeyIfNeeded(cache_.get(), key, -1, -1, slot);
  return s;
}

Status PikaCache::ZRevrange(std::string &key, int64_t start, int64_t stop, std::vector<storage::ScoreMember> *score_members,
                            const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  auto cache_obj = cache_.get();
  auto db_obj = slot->db();
  Status s;
  if (cache_obj->Exists(key)) {
    uint64_t cache_len = 0;
    cache_obj->ZCard(key, &cache_len);
    int32_t db_len = 0;
    db_obj->ZCard(key, &db_len);
    int64_t out_start;
    int64_t out_stop;
    RangeStatus rs = CheckCacheRevRange(cache_len, db_len, start, stop, out_start, out_stop);
    if (rs == RangeStatus::RangeHit) {
      return cache_obj->ZRevrange(key, out_start, out_stop, score_members);
    } else if (rs == RangeStatus::RangeMiss) {
      ReloadCacheKeyIfNeeded(cache_obj, key, cache_len, db_len);
      return Status::NotFound("key not in cache");
    } else if (rs == RangeStatus::RangeError) {
      return Status::NotFound("error revrange");
    } else {
      return Status::Corruption("unknown error");
    }
  } else {
    return Status::NotFound("key not in cache");
  }
}

Status PikaCache::ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                                   std::vector<storage::ScoreMember> *score_members, ZRevrangebyscoreCmd *cmd) {
  std::unique_lock l(rwlock_);
  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    storage::ScoreMember cache_min_sm;
    storage::ScoreMember cache_max_sm;
    if (!GetCacheMinMaxSM(cache_obj, key, cache_min_sm, cache_max_sm)) {
      return Status::NotFound("key not exist");
    }
    auto cache_min_score = cache_min_sm.score;
    auto cache_max_score = cache_max_sm.score;

    auto rs = CheckCacheRangeByScore(cache_len, cache_min_score, cache_max_score, cmd->MinScore(), cmd->MaxScore(),
                                     cmd->LeftClose(), cmd->RightClose());
    if (RangeStatus::RangeHit == rs) {
      return cache_obj->ZRevrangebyscore(key, min, max, score_members, cmd->Offset(), cmd->Count());
    } else if (RangeStatus::RangeMiss == rs) {
      ReloadCacheKeyIfNeeded(cache_obj, key, cache_len);
      return Status::NotFound("score range miss");
    } else {
      return Status::NotFound("score range error");
    }
  }
}

bool PikaCache::CacheSizeEqsDB(std::string &key, const std::shared_ptr<Slot> &slot) {
  int32_t db_len = 0;
  slot->DbRWLockWriter();
  slot->db()->ZCard(key, &db_len);
  slot->DbRWUnLock();
  std::unique_lock l(rwlock_);
  uint64_t cache_len = 0;
  cache_->ZCard(key, &cache_len);

  return db_len == (int32_t)cache_len;
}

Status PikaCache::ZRevrangebylex(std::string &key, std::string &min, std::string &max,
                                 std::vector<std::string> *members, const std::shared_ptr<Slot> &slot) {
  if (CacheSizeEqsDB(key, slot)) {
    std::unique_lock l(rwlock_);
    return cache_->ZRevrangebylex(key, min, max, members);
  } else {
    return Status::NotFound("key not in cache");
  }
}

Status PikaCache::ZRevrank(std::string &key, std::string &member, int64_t *rank, const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  auto cache_obj = cache_.get();
  uint64_t cache_len = 0;
  cache_obj->ZCard(key, &cache_len);
  if (cache_len <= 0) {
    return Status::NotFound("key not in cache");
  } else {
    auto s = cache_obj->ZRevrank(key, member, rank);
    if (s.ok()) {
      if (cache_start_pos_ == cache::CACHE_START_FROM_BEGIN) {
        int32_t db_len = 0;
        slot->db()->ZCard(key, &db_len);
        *rank = db_len - cache_len + *rank;
      }
      return s;
    } else {
      return Status::NotFound("member not in cache");
    }
  }
}
Status PikaCache::ZScore(std::string &key, std::string &member, double *score, const std::shared_ptr<Slot> &slot) {
  std::unique_lock l(rwlock_);
  auto s = cache_->ZScore(key, member, score);
  if (!s.ok()) {
    return Status::NotFound("key or member not in cache");
  }
  return s;
}

Status PikaCache::ZRangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members,
                              const std::shared_ptr<Slot> &slot) {
  if (CacheSizeEqsDB(key, slot)) {
    std::unique_lock l(rwlock_);
    return cache_->ZRangebylex(key, min, max, members);
  } else {
    return Status::NotFound("key not in cache");
  }
}

Status PikaCache::ZLexcount(std::string &key, std::string &min, std::string &max, uint64_t *len,
                            const std::shared_ptr<Slot> &slot) {
  if (CacheSizeEqsDB(key, slot)) {
    std::unique_lock l(rwlock_);
    return cache_->ZLexcount(key, min, max, len);
  } else {
    return Status::NotFound("key not in cache");
  }
}

Status PikaCache::ZRemrangebylex(std::string &key, std::string &min, std::string &max,
                                 const std::shared_ptr<Slot> &slot) {
  if (CacheSizeEqsDB(key, slot)) {
    std::unique_lock l(rwlock_);
    return cache_->ZRemrangebylex(key, min, max);
  } else {
    return Status::NotFound("key not in cache");
  }
}

/*-----------------------------------------------------------------------------
 * Bit Commands
 *----------------------------------------------------------------------------*/
Status PikaCache::SetBit(std::string &key, size_t offset, int64_t value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->SetBit(key, offset, value);
}

Status PikaCache::SetBitIfKeyExist(std::string &key, size_t offset, int64_t value) {
  std::unique_lock l(rwlock_);

  
  
  if (cache_->Exists(key)) {
    return cache_->SetBit(key, offset, value);
  }
  return Status::NotFound("key not exist");
}

Status PikaCache::GetBit(std::string &key, size_t offset, int64_t *value) {
  std::unique_lock l(rwlock_);

  
  
  return cache_->GetBit(key, offset, value);
}

Status PikaCache::BitCount(std::string &key, int64_t start, int64_t end, int64_t *value, bool have_offset) {
  std::unique_lock l(rwlock_);
  return cache_->BitCount(key, start, end, value, have_offset);
}

Status PikaCache::BitPos(std::string &key, int64_t bit, int64_t *value) {
  std::unique_lock l(rwlock_);
  return cache_->BitPos(key, bit, value);
}

Status PikaCache::BitPos(std::string &key, int64_t bit, int64_t start, int64_t *value) {
  std::unique_lock l(rwlock_);
  return cache_->BitPos(key, bit, start, value);
}

Status PikaCache::BitPos(std::string &key, int64_t bit, int64_t start, int64_t end, int64_t *value) {
  std::unique_lock l(rwlock_);
  return cache_->BitPos(key, bit, start, end, value);
}

Status PikaCache::InitWithoutLock() {
  cache_status_ = PIKA_CACHE_STATUS_INIT;
  if (cache_ == nullptr) {
    cache_ = std::make_unique<cache::RedisCache>();
  }
  Status s = cache_->Open();
  if (!s.ok()) {
    LOG(ERROR) << "PikaCache::InitWithoutLock Open cache failed";
    DestroyWithoutLock();
    cache_status_ = PIKA_CACHE_STATUS_NONE;
    return Status::Corruption("create redis cache failed");
  }
  cache_load_thread_ = std::make_unique<PikaCacheLoadThread>(cache_start_pos_, cache_items_per_key_, shared_from_this());
  cache_load_thread_->StartThread();
  cache_status_ = PIKA_CACHE_STATUS_OK;
  return Status::OK();
}

void PikaCache::DestroyWithoutLock(void) {
  cache_status_ = PIKA_CACHE_STATUS_DESTROY;
  cache_.reset();
}
Status PikaCache::WriteKvToCache(std::string &key, std::string &value, int64_t ttl) {
  if (0 >= ttl) {
    if (PIKA_TTL_NONE == ttl) {
      return SetnxWithoutTTL(key, value);
    } else {
      return Del({key});
    }
  } else {
    return Setnx(key, value, ttl);
  }
  return Status::OK();
}

Status PikaCache::WriteHashToCache(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl) {
  if (0 >= ttl) {
    if (PIKA_TTL_NONE == ttl) {
      return HMSetnxWithoutTTL(key, fvs);
    } else {
      return Del({key});
    }
  } else {
    return HMSetnx(key, fvs, ttl);
  }
  return Status::OK();
}

Status PikaCache::WriteListToCache(std::string &key, std::vector<std::string> &values, int64_t ttl) {
  if (0 >= ttl) {
    if (PIKA_TTL_NONE == ttl) {
      return RPushnxWithoutTTL(key, values);
    } else {
      return Del({key});
    }
  } else {
    return RPushnx(key, values, ttl);
  }
  return Status::OK();
}

Status PikaCache::WriteSetToCache(std::string &key, std::vector<std::string> &members, int64_t ttl) {
  if (0 >= ttl) {
    if (PIKA_TTL_NONE == ttl) {
      return SAddnxWithoutTTL(key, members);
    } else {
      return Del({key});
    }
  } else {
    return SAddnx(key, members, ttl);
  }
  return Status::OK();
}

Status PikaCache::WriteZSetToCache(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl) {
  if (0 >= ttl) {
    if (PIKA_TTL_NONE == ttl) {
      return ZAddnxWithoutTTL(key, score_members);
    } else {
      return Del({key});
    }
  } else {
    return ZAddnx(key, score_members, ttl);
  }
  return Status::OK();
}

void PikaCache::PushKeyToAsyncLoadQueue(const char key_type, std::string &key) {
  cache_load_thread_->Push(key_type, key);
}

void PikaCache::ClearHitRatio(void) {
  std::unique_lock l(rwlock_);
  cache::RedisCache::ResetHitAndMissNum();
}
