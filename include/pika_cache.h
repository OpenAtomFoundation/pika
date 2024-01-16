// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef PIKA_CACHE_H_
#define PIKA_CACHE_H_

#include <atomic>
#include <sstream>
#include <vector>

#include "cache/include/cache.h"
#include "include/pika_define.h"
#include "include/pika_server.h"
#include "include/pika_zset.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "storage/storage.h"

enum RangeStatus { RangeError = 1, RangeHit, RangeMiss };

struct CacheInfo {
  int status = PIKA_CACHE_STATUS_NONE;
  uint32_t cache_num = 0;
  int64_t keys_num = 0;
  size_t used_memory = 0;
  int64_t hits = 0;
  int64_t misses = 0;
  uint64_t async_load_keys_num = 0;
  uint32_t waitting_load_keys_num = 0;
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

class PikaCacheLoadThread;
class PikaCache : public pstd::noncopyable, public std::enable_shared_from_this<PikaCache> {
 public:
  PikaCache(int zset_cache_start_pos, int zset_cache_field_num_per_key);
  ~PikaCache();

  rocksdb::Status Init(uint32_t cache_num, cache::CacheConfig *cache_cfg);
  rocksdb::Status Reset(uint32_t cache_num, cache::CacheConfig *cache_cfg = nullptr);
  std::map<storage::DataType, int64_t> TTL(std::string &key, std::map<storage::DataType, rocksdb::Status> *type_status);
  void ResetConfig(cache::CacheConfig *cache_cfg);
  void Destroy(void);
  void SetCacheStatus(int status);
  int CacheStatus(void);
  void ClearHitRatio(void);
  // Normal Commands
  void Info(CacheInfo &info);
  long long DbSize(void);
  bool Exists(std::string &key);
  void FlushSlot(void);
  void ActiveExpireCycle();
  void ProcessCronTask(void);

  rocksdb::Status Del(const std::vector<std::string> &keys);
  rocksdb::Status Expire(std::string &key, int64_t ttl);
  rocksdb::Status Expireat(std::string &key, int64_t ttl);
  rocksdb::Status TTL(std::string &key, int64_t *ttl);
  rocksdb::Status Persist(std::string &key);
  rocksdb::Status Type(std::string &key, std::string *value);
  rocksdb::Status RandomKey(std::string *key);
  rocksdb::Status GetType(const std::string &key, bool single, std::vector<std::string> &types);

  // String Commands
  rocksdb::Status Set(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status Setnx(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetnxWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status Setxx(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetxxWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status MSet(const std::vector<storage::KeyValue> &kvs);
  rocksdb::Status Get(std::string &key, std::string *value);
  rocksdb::Status MGet(const std::vector<std::string> &keys, std::vector<storage::ValueStatus> *vss);
  rocksdb::Status Incrxx(std::string &key);
  rocksdb::Status Decrxx(std::string &key);
  rocksdb::Status IncrByxx(std::string &key, uint64_t incr);
  rocksdb::Status DecrByxx(std::string &key, uint64_t incr);
  rocksdb::Status Incrbyfloatxx(std::string &key, long double incr);
  rocksdb::Status Appendxx(std::string &key, std::string &value);
  rocksdb::Status GetRange(std::string &key, int64_t start, int64_t end, std::string *value);
  rocksdb::Status SetRangexx(std::string &key, int64_t start, std::string &value);
  rocksdb::Status Strlen(std::string &key, int32_t *len);

  // Hash Commands
  rocksdb::Status HDel(std::string &key, std::vector<std::string> &fields);
  rocksdb::Status HSet(std::string &key, std::string &field, std::string &value);
  rocksdb::Status HSetIfKeyExist(std::string &key, std::string &field, std::string &value);
  rocksdb::Status HSetIfKeyExistAndFieldNotExist(std::string &key, std::string &field, std::string &value);
  rocksdb::Status HMSet(std::string &key, std::vector<storage::FieldValue> &fvs);
  rocksdb::Status HMSetnx(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl);
  rocksdb::Status HMSetnxWithoutTTL(std::string &key, std::vector<storage::FieldValue> &fvs);
  rocksdb::Status HMSetxx(std::string &key, std::vector<storage::FieldValue> &fvs);
  rocksdb::Status HGet(std::string &key, std::string &field, std::string *value);
  rocksdb::Status HMGet(std::string &key, std::vector<std::string> &fields, std::vector<storage::ValueStatus> *vss);
  rocksdb::Status HGetall(std::string &key, std::vector<storage::FieldValue> *fvs);
  rocksdb::Status HKeys(std::string &key, std::vector<std::string> *fields);
  rocksdb::Status HVals(std::string &key, std::vector<std::string> *values);
  rocksdb::Status HExists(std::string &key, std::string &field);
  rocksdb::Status HIncrbyxx(std::string &key, std::string &field, int64_t value);
  rocksdb::Status HIncrbyfloatxx(std::string &key, std::string &field, long double value);
  rocksdb::Status HLen(std::string &key, uint64_t *len);
  rocksdb::Status HStrlen(std::string &key, std::string &field, uint64_t *len);

  // List Commands
  rocksdb::Status LIndex(std::string &key, int64_t index, std::string *element);
  rocksdb::Status LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot,
                          std::string &value);
  rocksdb::Status LLen(std::string &key, uint64_t *len);
  rocksdb::Status LPop(std::string &key, std::string *element);
  rocksdb::Status LPush(std::string &key, std::vector<std::string> &values);
  rocksdb::Status LPushx(std::string &key, std::vector<std::string> &values);
  rocksdb::Status LRange(std::string &key, int64_t start, int64_t stop, std::vector<std::string> *values);
  rocksdb::Status LRem(std::string &key, int64_t count, std::string &value);
  rocksdb::Status LSet(std::string &key, int64_t index, std::string &value);
  rocksdb::Status LTrim(std::string &key, int64_t start, int64_t stop);
  rocksdb::Status RPop(std::string &key, std::string *element);
  rocksdb::Status RPush(std::string &key, std::vector<std::string> &values);
  rocksdb::Status RPushx(std::string &key, std::vector<std::string> &values);
  rocksdb::Status RPushnx(std::string &key, std::vector<std::string> &values, int64_t ttl);
  rocksdb::Status RPushnxWithoutTTL(std::string &key, std::vector<std::string> &values);

  // Set Commands
  rocksdb::Status SAdd(std::string &key, std::vector<std::string> &members);
  rocksdb::Status SAddIfKeyExist(std::string &key, std::vector<std::string> &members);
  rocksdb::Status SAddnx(std::string &key, std::vector<std::string> &members, int64_t ttl);
  rocksdb::Status SAddnxWithoutTTL(std::string &key, std::vector<std::string> &members);
  rocksdb::Status SCard(std::string &key, uint64_t *len);
  rocksdb::Status SIsmember(std::string &key, std::string &member);
  rocksdb::Status SMembers(std::string &key, std::vector<std::string> *members);
  rocksdb::Status SRem(std::string &key, std::vector<std::string> &members);
  rocksdb::Status SRandmember(std::string &key, int64_t count, std::vector<std::string> *members);

  // ZSet Commands
  rocksdb::Status ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZAddIfKeyExist(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZAddnx(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  rocksdb::Status ZAddnxWithoutTTL(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZCard(std::string &key, uint32_t *len, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZCount(std::string &key, std::string &min, std::string &max, uint64_t *len, ZCountCmd *cmd);
  rocksdb::Status ZIncrby(std::string &key, std::string &member, double increment);
  rocksdb::Status ZIncrbyIfKeyExist(std::string &key, std::string &member, double increment, ZIncrbyCmd *cmd,
                                    const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRange(std::string &key, int64_t start, int64_t stop,
                         std::vector<storage::ScoreMember> *score_members, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRangebyscore(std::string &key, std::string &min, std::string &max,
                                std::vector<storage::ScoreMember> *score_members, ZRangebyscoreCmd *cmd);
  rocksdb::Status ZRank(std::string &key, std::string &member, int64_t *rank, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRem(std::string &key, std::vector<std::string> &members, std::shared_ptr<Slot> slot = nullptr);
  rocksdb::Status ZRemrangebyrank(std::string &key, std::string &min, std::string &max, int32_t ele_deleted = 0,
                                  const std::shared_ptr<Slot> &slot = nullptr);
  rocksdb::Status ZRemrangebyscore(std::string &key, std::string &min, std::string &max,
                                   const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrange(std::string &key, int64_t start, int64_t stop,
                            std::vector<storage::ScoreMember> *score_members, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                                   std::vector<storage::ScoreMember> *score_members, ZRevrangebyscoreCmd *cmd,
                                   const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrangebylex(std::string &key, std::string &min, std::string &max,
                                 std::vector<std::string> *members, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrank(std::string &key, std::string &member, int64_t *rank, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZScore(std::string &key, std::string &member, double *score, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members,
                              const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZLexcount(std::string &key, std::string &min, std::string &max, uint64_t *len,
                            const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRemrangebylex(std::string &key, std::string &min, std::string &max,
                                 const std::shared_ptr<Slot> &slot);

  // Bit Commands
  rocksdb::Status SetBit(std::string &key, size_t offset, int64_t value);
  rocksdb::Status SetBitIfKeyExist(std::string &key, size_t offset, int64_t value);
  rocksdb::Status GetBit(std::string &key, size_t offset, int64_t *value);
  rocksdb::Status BitCount(std::string &key, int64_t start, int64_t end, int64_t *value, bool have_offset);
  rocksdb::Status BitPos(std::string &key, int64_t bit, int64_t *value);
  rocksdb::Status BitPos(std::string &key, int64_t bit, int64_t start, int64_t *value);
  rocksdb::Status BitPos(std::string &key, int64_t bit, int64_t start, int64_t end, int64_t *value);

  // Cache
  rocksdb::Status WriteKVToCache(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status WriteHashToCache(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl);
  rocksdb::Status WriteListToCache(std::string &key, std::vector<std::string> &values, int64_t ttl);
  rocksdb::Status WriteSetToCache(std::string &key, std::vector<std::string> &members, int64_t ttl);
  rocksdb::Status WriteZSetToCache(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  void PushKeyToAsyncLoadQueue(const char key_type, std::string &key, const std::shared_ptr<Slot> &slot);
  rocksdb::Status CacheZCard(std::string &key, uint64_t *len);

 private:
  rocksdb::Status InitWithoutLock(uint32_t cache_num, cache::CacheConfig *cache_cfg);
  void DestroyWithoutLock(void);
  int CacheIndex(const std::string &key);
  RangeStatus CheckCacheRange(int32_t cache_len, int32_t db_len, int64_t start, int64_t stop, int64_t &out_start,
                              int64_t &out_stop);
  RangeStatus CheckCacheRevRange(int32_t cache_len, int32_t db_len, int64_t start, int64_t stop, int64_t &out_start,
                                 int64_t &out_stop);
  RangeStatus CheckCacheRangeByScore(uint64_t cache_len, double cache_min, double cache_max, double min, double max,
                                     bool left_close, bool right_close);
  bool CacheSizeEqsDB(std::string &key, const std::shared_ptr<Slot> &slot);
  void GetMinMaxScore(std::vector<storage::ScoreMember> &score_members, double &min, double &max);
  bool GetCacheMinMaxSM(cache::RedisCache *cache_obj, std::string &key, storage::ScoreMember &min_m,
                        storage::ScoreMember &max_m);
  bool ReloadCacheKeyIfNeeded(cache::RedisCache *cache_obj, std::string &key, int mem_len = -1, int db_len = -1,
                              const std::shared_ptr<Slot> &slot = nullptr);
  rocksdb::Status CleanCacheKeyIfNeeded(cache::RedisCache *cache_obj, std::string &key);

 private:
  std::atomic<int> cache_status_;
  uint32_t cache_num_ = 0;

  // currently only take effects to zset
  int zset_cache_start_pos_ = 0;
  int zset_cache_field_num_per_key_ = 0;
  std::shared_mutex rwlock_;
  std::unique_ptr<PikaCacheLoadThread> cache_load_thread_;
  std::vector<cache::RedisCache *> caches_;
  std::vector<std::shared_ptr<pstd::Mutex>> cache_mutexs_;
};

#endif