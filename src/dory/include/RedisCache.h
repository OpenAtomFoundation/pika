#ifndef __REDIS_CACHE_H__
#define __REDIS_CACHE_H__

#include <string>
#include <map>
#include <list>
#include <queue>
#include <vector>
#include <unistd.h>

extern "C" {
#include "redisdb/redisdbIF.h"
}

#include "RedisDef.h"
#include "pstd_status.h"
#include "storage/storage.h"

namespace dory {

using Status = pstd::Status;

class RedisCache
{
public:
    RedisCache();
    ~RedisCache();

    // Server APIs
    static void SetConfig(CacheConfig *cfg);
    static uint64_t GetUsedMemory(void);
    static void GetHitAndMissNum(long long *hits, long long *misses);
    static void ResetHitAndMissNum(void);
    Status Open(void);
    int ActiveExpireCycle(void);
    
    // Normal Commands
    bool Exists(std::string &key);
    long long DbSize(void);
    void FlushDb(void);

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
    Status Incr(std::string &key);
    Status Decr(std::string &key);
    Status IncrBy(std::string &key, long long incr);
    Status DecrBy(std::string &key, long long incr);
    Status Incrbyfloat(std::string &key, long double incr);
    Status Append(std::string &key, std::string &value);
    Status GetRange(std::string &key, int64_t start, int64_t end, std::string *value);
    Status SetRange(std::string &key, int64_t start, std::string &value);
    Status Strlen(std::string &key, int32_t *len);

    // Hash Commands
    Status HDel(std::string& key, std::vector<std::string> &fields);
    Status HSet(std::string &key, std::string &field, std::string &value);
    Status HSetnx(std::string &key, std::string &field, std::string &value);
    Status HMSet(std::string &key, std::vector<storage::FieldValue> &fvs);
    Status HGet(std::string &key, std::string &field, std::string *value);
    Status HMGet(std::string &key,
                 std::vector<std::string> &fields,
                 std::vector<storage::ValueStatus>* vss);
    Status HGetall(std::string &key, std::vector<storage::FieldValue> *fvs);
    Status HKeys(std::string &key, std::vector<std::string> *fields);
    Status HVals(std::string &key, std::vector<std::string> *values);
    Status HExists(std::string &key, std::string &field);
    Status HIncrby(std::string &key, std::string &field, int64_t value);
    Status HIncrbyfloat(std::string &key, std::string &field, long double value);
    Status HLen(std::string &key, unsigned long *len);
    Status HStrlen(std::string &key, std::string &field, unsigned long *len);

    // List Commands
    Status LIndex(std::string &key, long index, std::string *element);
    Status LInsert(std::string &key, storage::BeforeOrAfter &before_or_after,
                   std::string &pivot, std::string &value);
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

    // Set Commands
    Status SAdd(std::string &key, std::vector<std::string> &members);
    Status SCard(std::string &key, unsigned long *len);
    Status SIsmember(std::string &key, std::string &member);
    Status SMembers(std::string &key, std::vector<std::string> *members);
    Status SRem(std::string &key, std::vector<std::string> &members);
    Status SRandmember(std::string &key, long count, std::vector<std::string> *members);

    // Zset Commands
    Status ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members);
    Status ZCard(std::string &key, unsigned long *len);
    Status ZCount(std::string &key, std::string &min, std::string &max, unsigned long *len);
    Status ZIncrby(std::string &key, std::string &member, double increment);
    Status ZRange(std::string &key,
                  long start, long stop,
                  std::vector<storage::ScoreMember> *score_members);
    Status ZRangebyscore(std::string &key,
                         std::string &min, std::string &max,
                         std::vector<storage::ScoreMember> *score_members,
                         int64_t offset = 0, int64_t count = -1);
    Status ZRank(std::string &key, std::string &member, long *rank);
    Status ZRem(std::string &key, std::vector<std::string> &members);
    Status ZRemrangebyrank(std::string &key, std::string &min, std::string &max);
    Status ZRemrangebyscore(std::string &key, std::string &min, std::string &max);
    Status ZRevrange(std::string &key,
                     long start, long stop,
                     std::vector<storage::ScoreMember> *score_members);
    Status ZRevrangebyscore(std::string &key,
                            std::string &min, std::string &max,
                            std::vector<storage::ScoreMember> *score_members,
                            int64_t offset = 0, int64_t count = -1);
    Status ZRevrangebylex(std::string &key,
                          std::string &min, std::string &max,
                          std::vector<std::string> *members);
    Status ZRevrank(std::string &key, std::string &member, long *rank);
    Status ZScore(std::string &key, std::string &member, double *score);
    Status ZRangebylex(std::string &key,
                       std::string &min, std::string &max,
                       std::vector<std::string> *members);
    Status ZLexcount(std::string &key, std::string &min, std::string &max, unsigned long *len);
    Status ZRemrangebylex(std::string &key, std::string &min, std::string &max);

    // Bit Commands
    Status SetBit(std::string &key, size_t offset, long value);
    Status GetBit(std::string &key, size_t offset, long *value);
    Status BitCount(std::string &key, long start, long end, long *value, bool have_offset);
    Status BitPos(std::string &key, long bit, long *value);
    Status BitPos(std::string &key, long bit, long start, long *value);
    Status BitPos(std::string &key, long bit, long start, long end, long *value);

protected:
    void DecrObjectsRefCount(robj *argv1, robj *argv2 = NULL, robj *argv3 = NULL);
    void FreeSdsList(sds *items, unsigned int size);
    void FreeObjectList(robj **items, unsigned int size);
    void FreeHitemList(hitem *items, unsigned int size);
    void FreeZitemList(zitem *items, unsigned int size);
    void ConvertObjectToString(robj *obj, std::string *value);
    
private:
    RedisCache(const RedisCache&);
    RedisCache& operator=(const RedisCache&);

private:
    redisDbIF *m_RedisDB;
};

} // namespace dory 

#endif

/* EOF */
