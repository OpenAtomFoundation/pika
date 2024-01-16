// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef __CACHE_H__
#  define __CACHE_H__

#  include <unistd.h>

#  include <cstdint>
#  include <list>
#  include <map>
#  include <queue>
#  include <string>
#  include <vector>

extern "C" {
#  include "rediscache/redis.h"
}

#  include "config.h"
#  include "pstd_status.h"
#  include "storage/storage.h"

namespace cache {

using Status = rocksdb::Status;

class RedisCache {
 public:
  RedisCache();
  ~RedisCache();

  // Server APIs
  static void SetConfig(CacheConfig *cfg);
  static uint64_t GetUsedMemory(void);
  static void GetHitAndMissNum(int64_t *hits, int64_t *misses);
  static void ResetHitAndMissNum(void);
  Status Open(void);
  int32_t ActiveExpireCycle(void);

  // Normal Commands
  bool Exists(std::string &key);
  int64_t DbSize(void);
  void FlushDb(void);

  Status Del(const std::string &key);
  Status Expire(std::string &key, int64_t ttl);
  Status Expireat(std::string &key, int64_t ttl);
  Status TTL(std::string &key, int64_t *ttl);
  Status Persist(std::string &key);
  Status Type(std::string &key, std::string *value);
  Status RandomKey(std::string *key);

  // String Commands
  Status Set(std::string &key, std::string &value, int64_t ttl);
  Status SetWithoutTTL(std::string &key, std::string &value);
  Status Setnx(std::string &key, std::string &value, int64_t ttl);
  Status SetnxWithoutTTL(std::string &key, std::string &value);
  Status Setxx(std::string &key, std::string &value, int64_t ttl);
  Status SetxxWithoutTTL(std::string &key, std::string &value);
  Status Get(const std::string &key, std::string *value);
  Status Incr(std::string &key);
  Status Decr(std::string &key);
  Status IncrBy(std::string &key, int64_t incr);
  Status DecrBy(std::string &key, int64_t incr);
  Status Incrbyfloat(std::string &key, double incr);
  Status Append(std::string &key, std::string &value);
  Status GetRange(std::string &key, int64_t start, int64_t end, std::string *value);
  Status SetRange(std::string &key, int64_t start, std::string &value);
  Status Strlen(std::string &key, int32_t *len);

  // Hash Commands
  Status HDel(std::string &key, std::vector<std::string> &fields);
  Status HSet(std::string &key, std::string &field, std::string &value);
  Status HSetnx(std::string &key, std::string &field, std::string &value);
  Status HMSet(std::string &key, std::vector<storage::FieldValue> &fvs);
  Status HGet(std::string &key, std::string &field, std::string *value);
  Status HMGet(std::string &key, std::vector<std::string> &fields, std::vector<storage::ValueStatus> *vss);
  Status HGetall(std::string &key, std::vector<storage::FieldValue> *fvs);
  Status HKeys(std::string &key, std::vector<std::string> *fields);
  Status HVals(std::string &key, std::vector<std::string> *values);
  Status HExists(std::string &key, std::string &field);
  Status HIncrby(std::string &key, std::string &field, int64_t value);
  Status HIncrbyfloat(std::string &key, std::string &field, double value);
  Status HLen(std::string &key, uint64_t *len);
  Status HStrlen(std::string &key, std::string &field, uint64_t *len);

  // List Commands
  Status LIndex(std::string &key, int64_t index, std::string *element);
  Status LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot, std::string &value);
  Status LLen(std::string &key, uint64_t *len);
  Status LPop(std::string &key, std::string *element);
  Status LPush(std::string &key, std::vector<std::string> &values);
  Status LPushx(std::string &key, std::vector<std::string> &values);
  Status LRange(std::string &key, int64_t start, int64_t stop, std::vector<std::string> *values);
  Status LRem(std::string &key, int64_t count, std::string &value);
  Status LSet(std::string &key, int64_t index, std::string &value);
  Status LTrim(std::string &key, int64_t start, int64_t stop);
  Status RPop(std::string &key, std::string *element);
  Status RPush(std::string &key, std::vector<std::string> &values);
  Status RPushx(std::string &key, std::vector<std::string> &values);

  // Set Commands
  Status SAdd(std::string &key, std::vector<std::string> &members);
  Status SCard(std::string &key, uint64_t *len);
  Status SIsmember(std::string &key, std::string &member);
  Status SMembers(std::string &key, std::vector<std::string> *members);
  Status SRem(std::string &key, std::vector<std::string> &members);
  Status SRandmember(std::string &key, int64_t count, std::vector<std::string> *members);

  // Zset Commands
  Status ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members);
  Status ZCard(std::string &key, uint64_t *len);
  Status ZCount(std::string &key, std::string &min, std::string &max, uint64_t *len);
  Status ZIncrby(std::string &key, std::string &member, double increment);
  Status ZRange(std::string &key, int64_t start, int64_t stop, std::vector<storage::ScoreMember> *score_members);
  Status ZRangebyscore(std::string &key, std::string &min, std::string &max,
                       std::vector<storage::ScoreMember> *score_members, int64_t offset = 0, int64_t count = -1);
  Status ZRank(std::string &key, std::string &member, int64_t *rank);
  Status ZRem(std::string &key, std::vector<std::string> &members);
  Status ZRemrangebyrank(std::string &key, std::string &min, std::string &max);
  Status ZRemrangebyscore(std::string &key, std::string &min, std::string &max);
  Status ZRevrange(std::string &key, int64_t start, int64_t stop, std::vector<storage::ScoreMember> *score_members);
  Status ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                          std::vector<storage::ScoreMember> *score_members, int64_t offset = 0, int64_t count = -1);
  Status ZRevrangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members);
  Status ZRevrank(std::string &key, std::string &member, int64_t *rank);
  Status ZScore(std::string &key, std::string &member, double *score);
  Status ZRangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members);
  Status ZLexcount(std::string &key, std::string &min, std::string &max, uint64_t *len);
  Status ZRemrangebylex(std::string &key, std::string &min, std::string &max);

  // Bit Commands
  Status SetBit(std::string &key, size_t offset, int64_t value);
  Status GetBit(std::string &key, size_t offset, int64_t *value);
  Status BitCount(std::string &key, int64_t start, int64_t end, int64_t *value, bool have_offset);
  Status BitPos(std::string &key, int64_t bit, int64_t *value);
  Status BitPos(std::string &key, int64_t bit, int64_t start, int64_t *value);
  Status BitPos(std::string &key, int64_t bit, int64_t start, int64_t end, int64_t *value);

 protected:
  void DecrObjectsRefCount(robj *argv1, robj *argv2 = nullptr, robj *argv3 = nullptr);
  void FreeSdsList(sds *items, uint32_t size);
  void FreeObjectList(robj **items, uint32_t size);
  void FreeHitemList(hitem *items, uint32_t size);
  void FreeZitemList(zitem *items, uint32_t size);
  void ConvertObjectToString(robj *obj, std::string *value);

 private:
  RedisCache(const RedisCache &);
  RedisCache &operator=(const RedisCache &);

 private:
  redisCache cache_;
};

}  // namespace cache

#endif

/* EOF */
