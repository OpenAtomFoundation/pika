#ifndef PIKA_PIKA_CACHE_H
#define PIKA_PIKA_CACHE_H

#include <atomic>
#include <sstream>
#include <vector>

#include "include/pika_server.h"
#include "include/pika_define.h"
#include "include/pika_zset.h"
#include "pstd/include/pstd_mutex.h"
#include "pstd/include/pstd_status.h"
#include "dory/include/RedisCache.h"
#include "storage/storage.h"

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
class PikaCache : public pstd::noncopyable, public std::enable_shared_from_this<PikaCache> {
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

  rocksdb::Status Init();
  rocksdb::Status Reset();
  void ResetConfig(dory::CacheConfig *cache_cfg);
  void Destroy(void);
  void SetCacheStatus(int status);
  int CacheStatus(void);

  // Normal Commands
  CacheInfo Info();
  bool Exists(std::string &key);
  void FlushSlot(void);
  void ActiveExpireCycle();
  double HitRatio(void);
  void ClearHitRatio(void);
  rocksdb::Status Del(const std::vector<std::string> &keys);
  rocksdb::Status Expire(std::string &key, int64_t ttl);
  rocksdb::Status Expireat(std::string &key, int64_t ttl);
  rocksdb::Status TTL(std::string &key, int64_t *ttl);
  rocksdb::Status Persist(std::string &key);
  rocksdb::Status Type(std::string &key, std::string *value);
  rocksdb::Status RandomKey(std::string *key);

  // String Commands
  rocksdb::Status Set(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status Setnx(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetnxWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status Setxx(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status SetxxWithoutTTL(std::string &key, std::string &value);
  rocksdb::Status MSet(const std::vector<storage::KeyValue>& kvs);
  rocksdb::Status Get(std::string &key, std::string *value);
  rocksdb::Status MGet(const std::vector<std::string>& keys, std::vector<storage::ValueStatus>* vss);
  rocksdb::Status Incrxx(std::string &key);
  rocksdb::Status Decrxx(std::string &key);
  rocksdb::Status IncrByxx(std::string &key, long long incr);
  rocksdb::Status DecrByxx(std::string &key, long long incr);
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
  rocksdb::Status HLen(std::string &key, unsigned long *len);
  rocksdb::Status HStrlen(std::string &key, std::string &field, unsigned long *len);

  // List Commands
  rocksdb::Status LIndex(std::string &key, long index, std::string *element);
  rocksdb::Status LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot, std::string &value);
  rocksdb::Status LLen(std::string &key, unsigned long *len);
  rocksdb::Status LPop(std::string &key, std::string *element);
  rocksdb::Status LPush(std::string &key, std::vector<std::string> &values);
  rocksdb::Status LPushx(std::string &key, std::vector<std::string> &values);
  rocksdb::Status LRange(std::string &key, long start, long stop, std::vector<std::string> *values);
  rocksdb::Status LRem(std::string &key, long count, std::string &value);
  rocksdb::Status LSet(std::string &key, long index, std::string &value);
  rocksdb::Status LTrim(std::string &key, long start, long stop);
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
  rocksdb::Status SCard(std::string &key, unsigned long *len);
  rocksdb::Status SIsmember(std::string &key, std::string &member);
  rocksdb::Status SMembers(std::string &key, std::vector<std::string> *members);
  rocksdb::Status SRem(std::string &key, std::vector<std::string> &members);
  rocksdb::Status SRandmember(std::string &key, long count, std::vector<std::string> *members);

  // ZSet Commands
  rocksdb::Status ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZAddIfKeyExist(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZAddnx(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  rocksdb::Status ZAddnxWithoutTTL(std::string &key, std::vector<storage::ScoreMember> &score_members);
  rocksdb::Status ZCard(std::string &key, unsigned long *len, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZCount(std::string &key, std::string &min, std::string &max, unsigned long *len, ZCountCmd *cmd);
  rocksdb::Status ZIncrby(std::string &key, std::string &member, double increment);
  rocksdb::Status ZIncrbyIfKeyExist(std::string &key, std::string &member, double increment, ZIncrbyCmd *cmd);
  rocksdb::Status ZRange(std::string &key, long start, long stop, std::vector<storage::ScoreMember> *score_members,
                const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRangebyscore(std::string &key, std::string &min, std::string &max,
                       std::vector<storage::ScoreMember> *score_members, ZRangebyscoreCmd *cmd);
  rocksdb::Status ZRank(std::string &key, std::string &member, long *rank, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRem(std::string &key, std::vector<std::string> &members, std::shared_ptr<Slot> slot = nullptr);
  rocksdb::Status ZRemrangebyrank(std::string &key, std::string &min, std::string &max, int32_t ele_deleted = 0,
                         const std::shared_ptr<Slot> &slot = nullptr);
  rocksdb::Status ZRemrangebyscore(std::string &key, std::string &min, std::string &max, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrange(std::string &key, long start, long stop, std::vector<storage::ScoreMember> *score_members,
                   const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                          std::vector<storage::ScoreMember> *score_members, ZRevrangebyscoreCmd *cmd);
  rocksdb::Status ZRevrangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members,
                        const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRevrank(std::string &key, std::string &member, long *rank, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZScore(std::string &key, std::string &member, double *score, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRangebylex(std::string &key, std::string &min, std::string &max, std::vector<std::string> *members, const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZLexcount(std::string &key, std::string &min, std::string &max, unsigned long *len,
                   const std::shared_ptr<Slot> &slot);
  rocksdb::Status ZRemrangebylex(std::string &key, std::string &min, std::string &max, const std::shared_ptr<Slot> &slot);

  // Bit Commands
  rocksdb::Status SetBit(std::string &key, size_t offset, long value);
  rocksdb::Status SetBitIfKeyExist(std::string &key, size_t offset, long value);
  rocksdb::Status GetBit(std::string &key, size_t offset, long *value);
  rocksdb::Status BitCount(std::string &key, long start, long end, long *value, bool have_offset);
  rocksdb::Status BitPos(std::string &key, long bit, long *value);
  rocksdb::Status BitPos(std::string &key, long bit, long start, long *value);
  rocksdb::Status BitPos(std::string &key, long bit, long start, long end, long *value);

  // Cache
  rocksdb::Status WriteKvToCache(std::string &key, std::string &value, int64_t ttl);
  rocksdb::Status WriteHashToCache(std::string &key, std::vector<storage::FieldValue> &fvs, int64_t ttl);
  rocksdb::Status WriteListToCache(std::string &key, std::vector<std::string> &values, int64_t ttl);
  rocksdb::Status WriteSetToCache(std::string &key, std::vector<std::string> &members, int64_t ttl);
  rocksdb::Status WriteZSetToCache(std::string &key, std::vector<storage::ScoreMember> &score_members, int64_t ttl);
  void PushKeyToAsyncLoadQueue(const char key_type, std::string &key);
//  static bool CheckCacheDBScoreMembers(std::vector<storage::ScoreMember> &cache_score_members,
//                                       std::vector<storage::ScoreMember> &db_score_members, bool print_result = true);
  rocksdb::Status CacheZCard(std::string &key, unsigned long *len);

  std::shared_ptr<Slot> GetSlot() { return slot_; }
 private:
  rocksdb::Status InitWithoutLock();
  void DestroyWithoutLock(void);
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
  rocksdb::Status CleanCacheKeyIfNeeded(dory::RedisCache *cache_obj, std::string &key);

 private:
  std::atomic<int> cache_status_;
  std::vector<dory::RedisCache*> caches_;
  std::vector<pstd::Mutex*> cache_mutexs_;
  // currently only take effects to zset
  int cache_start_pos_;
  int cache_items_per_key_;
  std::shared_mutex rwlock_;
  std::unique_ptr<PikaCacheLoadThread> cache_load_thread_;
  std::shared_ptr<Slot> slot_;
};

#endif