#ifndef __REDIS_DB_IF_H__
#define __REDIS_DB_IF_H__

#include <stdint.h>

#ifdef _cplusplus
extern "C" {
#endif

#include "commondef.h"
#include "object.h"
#include "zmalloc.h"

// redis db handle
typedef void redisDbIF;

// hash value
typedef struct _hitem {
    sds field;
    sds value;
    int status;
} hitem;

// zset member
typedef struct _zitem {
    double score;
    sds member;
} zitem;

/*-----------------------------------------------------------------------------
 * Server APIS
 *----------------------------------------------------------------------------*/
void RsSetConfig(db_config* cfg);
redisDbIF* RsCreateDbHandle(void);
void RsDestroyDbHandle(redisDbIF *db);
int RsFreeMemoryIfNeeded(redisDbIF *db);
int RsActiveExpireCycle(redisDbIF *db);
size_t RsGetUsedMemory(void);
void RsGetHitAndMissNum(long long *hits, long long *misses);
void RsResetHitAndMissNum(void);

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
int RsExpire(redisDbIF *db, robj *key, robj *expire);
int RsExpireat(redisDbIF *db, robj *key, robj *expire);
int RsTTL(redisDbIF *db, robj *key, int64_t *ttl);
int RsPersist(redisDbIF *db, robj *key);
int RsType(redisDbIF *db, robj *key, sds *val);
int RsDel(redisDbIF *db, robj *key);
int RsExists(redisDbIF *db, robj *key);
int RsDbSize(redisDbIF *db, long long *dbsize);
int RsFlushDb(redisDbIF *db);
int RsRandomkey(redisDbIF *db, sds *key);

/*-----------------------------------------------------------------------------
 * String Commands
 *----------------------------------------------------------------------------*/
int RsSet(redisDbIF *db, robj *key, robj *val, robj *expire);
int RsSetnx(redisDbIF *db, robj *key, robj *val, robj *expire);
int RsSetxx(redisDbIF *db, robj *key, robj *val, robj *expire);
int RsGet(redisDbIF *db, robj *key, robj **val);
int RsIncr(redisDbIF *db, robj *key, long long *ret);
int RsDecr(redisDbIF *db, robj *key, long long *ret);
int RsIncrBy(redisDbIF *db, robj *key, long long incr, long long *ret);
int RsDecrBy(redisDbIF *db, robj *key, long long incr, long long *ret);
int RsIncrByFloat(redisDbIF *db, robj *key, long double incr, long double *ret);
int RsAppend(redisDbIF *db, robj *key, robj *val, unsigned long *ret);
int RsGetRange(redisDbIF *db, robj *key, long start, long end, sds *val);
int RsSetRange(redisDbIF *db, robj *key, long start, robj *val, unsigned long *ret);
int RsStrlen(redisDbIF *db, robj *key, int *val_len);

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/
int RsHDel(redisDbIF *db, robj *key, robj *fields[], unsigned long fields_size, unsigned long *ret);
int RsHSet(redisDbIF *db, robj *key, robj *field, robj *val);
int RsHSetnx(redisDbIF *db, robj *key, robj *field, robj *val);
int RsHMSet(redisDbIF *db, robj *key, robj *items[], unsigned long items_size);
int RsHGet(redisDbIF *db, robj *key, robj *field, sds *val);
int RsHMGet(redisDbIF *db, robj *key, hitem *items, unsigned long items_size);
int RsHGetAll(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size);
int RsHKeys(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size);
int RsHVals(redisDbIF *db, robj *key, hitem **items, unsigned long *items_size);
int RsHExists(redisDbIF *db, robj *key, robj *field, int *is_exist);
int RsHIncrby(redisDbIF *db, robj *key, robj *field, long long val, long long *ret);
int RsHIncrbyfloat(redisDbIF *db, robj *key, robj *field, long double val, long double *ret);
int RsHlen(redisDbIF *db, robj *key, unsigned long *len);
int RsHStrlen(redisDbIF *db, robj *key, robj *field, unsigned long *len);

/*-----------------------------------------------------------------------------
 * List Commands
 *----------------------------------------------------------------------------*/
int RsLIndex(redisDbIF *db, robj *key, long index, sds *element);
int RsLInsert(redisDbIF *db, robj *key, int where, robj *pivot, robj *val);
int RsLLen(redisDbIF *db, robj *key, unsigned long *len);
int RsLPop(redisDbIF *db, robj *key, sds *element);
int RsLPush(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size);
int RsLPushx(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size);
int RsLRange(redisDbIF *db, robj *key, long start, long end, sds **vals, unsigned long *vals_size);
int RsLRem(redisDbIF *db, robj *key, long count, robj *val);
int RsLSet(redisDbIF *db, robj *key, long index, robj *val);
int RsLTrim(redisDbIF *db, robj *key, long start, long end);
int RsRPop(redisDbIF *db, robj *key, sds *element);
int RsRPush(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size);
int RsRPushx(redisDbIF *db, robj *key, robj *vals[], unsigned long vals_size);

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/
int RsSAdd(redisDbIF *db, robj *key, robj *members[], unsigned long members_size);
int RsSCard(redisDbIF *db, robj *key, unsigned long *len);
int RsSIsmember(redisDbIF *db, robj *key, robj *member, int *is_member);
int RsSMembers(redisDbIF *db, robj *key, sds **members, unsigned long *members_size);
int RsSRem(redisDbIF *db, robj *key, robj *members[], unsigned long members_size);
int RsSRandmember(redisDbIF *db, robj *key, long l, sds **members, unsigned long *members_size);

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/
int RsZAdd(redisDbIF *db, robj *key, robj *items[], unsigned long items_size);
int RsZCard(redisDbIF *db, robj *key, unsigned long *len);
int RsZCount(redisDbIF *db, robj *key, robj *min, robj *max, unsigned long *len);
int RsZIncrby(redisDbIF *db, robj *key, robj *items[], unsigned long items_size);
int RsZrange(redisDbIF *db, robj *key, long start, long end, zitem **items, unsigned long *items_size);
int RsZRangebyscore(redisDbIF *db, robj *key, robj *min, robj *max, zitem **items, unsigned long *items_size, long offset, long count);
int RsZRank(redisDbIF *db, robj *key, robj *member, long *rank);
int RsZRem(redisDbIF *db, robj *key, robj *members[], unsigned long members_size);
int RsZRemrangebyrank(redisDbIF *db, robj *key, robj *min, robj *max);
int RsZRemrangebyscore(redisDbIF *db, robj *key, robj *min, robj *max);
int RsZRevrange(redisDbIF *db, robj *key, long start, long end, zitem **items, unsigned long *items_size);
int RsZRevrangebyscore(redisDbIF *db, robj *key, robj *min, robj *max, zitem **items, unsigned long *items_size, long offset, long count);
int RsZRevrangebylex(redisDbIF *db, robj *key, robj *min, robj *max, sds **members, unsigned long *members_size);
int RsZRevrank(redisDbIF *db, robj *key, robj *member, long *rank);
int RsZScore(redisDbIF *db, robj *key, robj *member, double *score);
int RsZRangebylex(redisDbIF *db, robj *key, robj *min, robj *max, sds **members, unsigned long *members_size);
int RsZLexcount(redisDbIF *db, robj *key, robj *min, robj *max, unsigned long *len);
int RsZRemrangebylex(redisDbIF *db, robj *key, robj *min, robj *max);

/*-----------------------------------------------------------------------------
 * Bit Commands
 *----------------------------------------------------------------------------*/
int RsSetBit(redisDbIF *db, robj *key, size_t bitoffset, long on);
int RsGetBit(redisDbIF *db, robj *key, size_t bitoffset, long *val);
int RsBitCount(redisDbIF *db, robj *key, long start, long end, long *val, int have_offset);
int RsBitPos(redisDbIF *db, robj *key, long bit, long start, long end, long *val, int offset_status);

#ifdef _cplusplus
}
#endif

#endif
