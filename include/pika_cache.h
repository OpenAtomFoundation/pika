#ifndef PIKA_PIKA_CACHE_H
#define PIKA_PIKA_CACHE_H

#include <atomic>
#include <sstream>
#include <vector>

#include "include/pika_server.h"
#include "pika_define.h"
#include "pika_zset.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "dory/include/RedisCache.h"
#include "storage/storage.h"

using Status = pstd::Status;
/*
 * cache status
 */
const int PIKA_CACHE_STATUS_NONE = 0;
const int PIKA_CACHE_STATUS_INIT = 1;
const int PIKA_CACHE_STATUS_OK = 2;
const int PIKA_CACHE_STATUS_RESET = 3;
const int PIKA_CACHE_STATUS_DESTROY = 4;
const int PIKA_CACHE_STATUS_CLEAR = 5;
const int CACHE_START_FROM_BEGIN = 0;
const int CACHE_START_FROM_END = -1;
/*
 * key type
 */
const char PIKA_KEY_TYPE_KV = 'k';
const char PIKA_KEY_TYPE_HASH = 'h';
const char PIKA_KEY_TYPE_SET = 's';
const char PIKA_KEY_TYPE_ZSET = 'z';

enum RangeStatus : int { RangeError = 1, RangeHit, RangeMiss };

class PikaCacheLoadThread;
class PikaCache : public pstd::noncopyable, std::enable_shared_from_this<PikaCache> {
 public:
  struct CacheInfo {
    int status;
    uint32_t cache_num;
    long long keys_num;
    size_t used_memory;
    long long hits;
    long long misses;
    uint64_t async_load_keys_num;
    uint32_t waitting_load_keys_num;
    CacheInfo()
        : status(PIKA_CACHE_STATUS_NONE),
          cache_num(0),
          keys_num(0),
          used_memory(0),
          hits(0),
          misses(0),
          async_load_keys_num(0),
          waitting_load_keys_num(0) {}
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

  PikaCache(int cache_start_pos_, int cache_items_per_key, std::shared_ptr<Slot> slot);
  ~PikaCache();

  Status Init(uint32_t cache_num, dory::CacheConfig *cache_cfg);
  Status Reset(uint32_t cache_num, dory::CacheConfig *cache_cfg = NULL);
  void ResetConfig(dory::CacheConfig *cache_cfg);
  void Destroy(void);
  void ProcessCronTask(void);
  void SetCacheStatus(int status);
  int CacheStatus(void);

  // Normal Commands
  void Info(CacheInfo &info);
  long long DbSize(void);
  bool Exists(std::string &key);
  void FlushSlot(void);
  void ActiveExpireCycle();

  Status Del(std::string &key);
  Status Expire(std::string &key, int64_t ttl);
  Status Expireat(std::string &key, int64_t ttl);
  Status TTL(std::string &key, int64_t *ttl);
  Status Persist(std::string &key);
  Status Type(std::string &key, std::string *value);
  Status RandomKey(std::string *key);

  // String Commands
  Status Set(std::string &key, std::string &value, int64_t ttl);
  Status Setnx(std::string &key, std::string &value, int64_t ttl);
  Status SetnxWithoutTTL(std::string &key, std::string &value);
  Status Setxx(std::string &key, std::string &value, int64_t ttl);
  Status SetxxWithoutTTL(std::string &key, std::string &value);
  Status Get(std::string &key, std::string *value);
  Status Incrxx(std::string &key);
  Status Decrxx(std::string &key);
  Status IncrByxx(std::string &key, long long incr);
  Status DecrByxx(std::string &key, long long incr);
  Status Incrbyfloatxx(std::string &key, long double incr);
  Status Appendxx(std::string &key, std::string &value);
  Status GetRange(std::string &key, int64_t start, int64_t end, std::string *value);
  Status SetRangexx(std::string &key, int64_t start, std::string &value);
  Status Strlen(std::string &key, int32_t *len);

  // Hash Commands
  Status HDel(std::string &key, std::vector<std::string> &fields);
  Status HSet(std::string &key, std::string &field, std::string &value);
  Status HSetIfKeyExist(std::string &key, std::string &field, std::string &value);
  Status HSetIfKeyExistAndFieldNotExist(std::string &key, std::string &field, std::string &value);
  Status HMSet(std::string &key, std::vector<storage::FieldValue> &fvs);
  Status HMSetnx(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl);
  Status HMSetnxWithoutTTL(std::string &key, std::vector<storage::FieldValue> &fvs);
  Status HMSetxx(std::string &key, std::vector<storage::FieldValue> &fvs);
  Status HGet(std::string &key, std::string &field, std::string *value);
  Status HMGet(std::string &key, std::vector<std::string> &fields, std::vector<storage::ValueStatus> *vss);
  Status HGetall(std::string &key, std::vector<storage::FieldValue> *fvs);
  Status HKeys(std::string &key, std::vector<std::string> *fields);
  Status HVals(std::string &key, std::vector<std::string> *values);
  Status HExists(std::string &key, std::string &field);
  Status HIncrbyxx(std::string &key, std::string &field, int64_t value);
  Status HIncrbyfloatxx(std::string &key, std::string &field, long double value);
  Status HLen(std::string &key, unsigned long *len);
  Status HStrlen(std::string &key, std::string &field, unsigned long *len);

  // List Commands
  Status LIndex(std::string &key, long index, std::string *element);
  Status LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot, std::string &value);
  Status LLen(std::string &key, unsigned long *len);
  Status LPop(std::string &key, std::string *element);
  Status LPush(std::string &key, std::vector<std::string> &values);
  Status LPushx(std::string &key, std::vector<std::string> &values);
  Status LRange(std::string &key, long start, long stop, std::vector<std::string> *values);
  Status LRem(std::string &key, long count, std::string &value);
  Status LSet(std::string &key, long index, std::string &value);
  Status LTrim(std::string &key, long start, long stop);
  Status RPop(std::string &key, std::string *element);
  Status RPush(std::string &key, std::vector<std::string> &values);
  Status RPushx(std::string &key, std::vector<std::string> &values);
  Status RPushnx(std::string &key, std::vector<std::string> &values, int64_t ttl);
  Status RPushnxWithoutTTL(std::string &key, std::vector<std::string> &values);

  // Set Commands
  Status SAdd(std::string &key, std::vector<std::string> &members);
  Status SAddIfKeyExist(std::string &key, std::vector<std::string> &members);
  Status SAddnx(std::string &key, std::vector<std::string> &members, int64_t ttl);
  Status SAddnxWithoutTTL(std::string &key, std::vector<std::string> &members);
  Status SCard(std::string &key, unsigned long *len);
  Status SIsmember(std::string &key, std::string &member);
  Status SMembers(std::string &key, std::vector<std::string> *members);
  Status SRem(std::string &key, std::vector<std::string> &members);
  Status SRandmember(std::string &key, long count, std::vector<std::string> *members);

  // ZSet Commands
  Status ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members);
  Status ZAddIfKeyExist(std::string &key, std::vector<storage::ScoreMember> &score_members);
  Status ZAddnx(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  Status ZAddnxWithoutTTL(std::string &key, std::vector<storage::ScoreMember> &score_members);
  Status ZCard(std::string &key, unsigned long *len, const std::shared_ptr<Slot> &slot);
  Status ZCount(std::string &key, std::string &min, std::string &max, unsigned long *len, ZCountCmd *cmd);
  Status ZIncrby(std::string &key, std::string &member, double increment);
  Status ZIncrbyIfKeyExist(std::string &key, std::string &member, double increment, ZIncrbyCmd *cmd);
  Status ZRange(std::string &key, long start, long stop, std::vector<storage::ScoreMember> *score_members,
                const std::shared_ptr<Slot> &slot);
  Status ZRangebyscore(std::string &key, std::string &min, std::string &max,
                       std::vector<storage::ScoreMember> *score_members, ZRangebyscoreCmd *cmd);
  Status ZRank(std::string &key, std::string &member, long *rank, const std::shared_ptr<Slot> &slot);
  Status ZRem(std::string &key, std::vector<std::string> &members, std::shared_ptr<Slot> slot = nullptr);
  Status ZRemrangebyrank(std::string &key, std::string &min, std::string &max, int32_t ele_deleted = 0,
                         const std::shared_ptr<Slot> &slot = nullptr);
  Status ZRemrangebyscore(std::string &key, std::string &min, std::string &max, const std::shared_ptr<Slot> &slot);
  Status ZRevrange(std::string &key, long start, long stop, std::vector<storage::ScoreMember> *score_members,
                   const std::shared_ptr<Slot> &slot);
  Status ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                          std::vector<storage::ScoreMember> *score_members, ZRevrangebyscoreCmd *cmd);
  Status ZRevrangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members,
                        const std::shared_ptr<Slot> &slot);
  Status ZRevrank(std::string &key, std::string &member, long *rank, const std::shared_ptr<Slot> &slot);
  Status ZScore(std::string &key, std::string &member, double *score, const std::shared_ptr<Slot> &slot);
  Status ZRangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members, const std::shared_ptr<Slot> &slot);
  Status ZLexcount(std::string &key, std::string &min, std::string &max, unsigned long *len,
                   const std::shared_ptr<Slot> &slot);
  Status ZRemrangebylex(std::string &key, std::string &min, std::string &max, const std::shared_ptr<Slot> &slot);

  // Bit Commands
  Status SetBit(std::string &key, size_t offset, long value);
  Status SetBitIfKeyExist(std::string &key, size_t offset, long value);
  Status GetBit(std::string &key, size_t offset, long *value);
  Status BitCount(std::string &key, long start, long end, long *value, bool have_offset);
  Status BitPos(std::string &key, long bit, long *value);
  Status BitPos(std::string &key, long bit, long start, long *value);
  Status BitPos(std::string &key, long bit, long start, long end, long *value);

  // Cache
  Status WriteKvToCache(std::string &key, std::string &value, int64_t ttl);
  Status WriteHashToCache(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl);
  Status WriteListToCache(std::string &key, std::vector<std::string> &values, int64_t ttl);
  Status WriteSetToCache(std::string &key, std::vector<std::string> &members, int64_t ttl);
  Status WriteZSetToCache(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  void PushKeyToAsyncLoadQueue(const char key_type, std::string &key);
  static bool CheckCacheDBScoreMembers(std::vector<storage::ScoreMember> &cache_score_members,
                                       std::vector<storage::ScoreMember> &db_score_members, bool print_result = true);
  Status CacheZCard(std::string &key, unsigned long *len);
  Status Select(int db_id);

  std::shared_ptr<Slot> GetSlot() { return slot_; }
 private:
  Status InitWithoutLock(uint32_t cache_num, dory::CacheConfig *cache_cfg);
  void DestroyWithoutLock(void);
  int CacheIndex(const std::string &key);
  RangeStatus CheckCacheRange(int32_t cache_len, int32_t db_len, long start, long stop, long &out_start,
                              long &out_stop);
  RangeStatus CheckCacheRevRange(int32_t cache_len, int32_t db_len, long start, long stop, long &out_start,
                                 long &out_stop);
  RangeStatus CheckCacheRangeByScore(unsigned long cache_len, double cache_min, double cache_max, double min,
                                     double max, bool left_close, bool right_close);
  bool CacheSizeEqsDB(std::string &key, const std::shared_ptr<Slot> &slot);
  void GetMinMaxScore(std::vector<storage::ScoreMember> &score_members, double &min, double &max);
  bool GetCacheMinMaxSM(dory::RedisCache *cache_obj, std::string &key, storage::ScoreMember &min_m,
                        storage::ScoreMember &max_m);
  bool ReloadCacheKeyIfNeeded(dory::RedisCache *cache_obj, std::string &key, int mem_len = -1, int db_len = -1,
                              const std::shared_ptr<Slot> &slot = nullptr);
  Status CleanCacheKeyIfNeeded(dory::RedisCache *cache_obj, std::string &key);

 private:
  std::atomic<int> cache_status_;
  std::unique_ptr<dory::RedisCache> cache_;
  uint32_t cache_num_;

  // currently only take effects to zset
  int cache_start_pos_;
  int cache_items_per_key_;
  std::shared_mutex rwlock_;
  PikaCacheLoadThread *cache_load_thread_;  // 这个线程保留
  std::shared_ptr<Slot> slot_;
};

#endif