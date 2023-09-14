#ifndef __DB_H__
#define __DB_H__

#include "object.h"
#include "dict.h"
#include "evict.h"

#ifdef _cplusplus
extern "C" {
#endif

#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1<<0)

// RedisDb 
typedef struct redisDb {
    dict *dict;                                 /* The keyspace for this DB */
    dict *expires;                              /* Timeout of keys with a timeout set */
    struct evictionPoolEntry *eviction_pool;    /* Eviction pool of keys */
} redisDb;

redisDb* createRedisDb(void);
void closeRedisDb(redisDb *db);
robj *lookupKey(redisDb *db, robj *key, int flags);
robj *lookupKeyRead(redisDb *db, robj *key);
robj *lookupKeyWrite(redisDb *db, robj *key);
void dbAdd(redisDb *db, robj *key, robj *val);
void dbOverwrite(redisDb *db, robj *key, robj *val);
void setKey(redisDb *db, robj *key, robj *val);
int dbExists(redisDb *db, robj *key);
robj *dbRandomKey(redisDb *db);
int dbDelete(redisDb *db, robj *key);
long long emptyDb(redisDb *db, void(callback)(void*));
int removeExpire(redisDb *db, robj *key);
void setExpire(redisDb *db, robj *key, long long when);
long long getExpire(redisDb *db, robj *key);
int expireIfNeeded(redisDb *db, robj *key);
int freeMemoryIfNeeded(redisDb *db);
int activeExpireCycle(redisDb *db);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);

#ifdef _cplusplus
}
#endif

#endif