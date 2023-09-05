#include <stdlib.h>
#include <string.h>

#include "redisdbIF.h"
#include "commondef.h"
#include "commonfunc.h"
#include "atomicvar.h"
#include "zmalloc.h"
#include "db.h"
#include "object.h"
#include "sds.h"
#include "dict.h"


db_config g_db_config;
db_status g_db_status;

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the commad second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds. */
static int expireGenericCommand(redisDb *redis_db, robj *kobj, robj *expire, long long basetime, int unit) {
    long long when; /* unix time in milliseconds when the key will expire. */

    if (getLongLongFromObject(expire, &when) != C_OK)
        return REDIS_INVALID_ARG;

    if (unit == UNIT_SECONDS) when *= 1000;
    when += basetime;

    /* No key, return zero. */
    if (lookupKeyWrite(redis_db,kobj) == NULL) {
        return REDIS_KEY_NOT_EXIST;
    }

    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we take the other branch of the IF statement setting an expire
     * (possibly in the past) and wait for an explicit DEL from the master. */
    if (when <= mstime()) {
        dbDelete(redis_db, kobj);
    } else {
        setExpire(redis_db, kobj, when);
    }
    return C_OK;
}

void RsSetConfig(db_config* cfg)
{
    if (NULL == cfg) return;

    atomicSet(g_db_config.maxmemory,cfg->maxmemory);
    atomicSet(g_db_config.maxmemory_policy,cfg->maxmemory_policy);
    atomicSet(g_db_config.maxmemory_samples,cfg->maxmemory_samples);
    atomicSet(g_db_config.lfu_decay_time,cfg->lfu_decay_time);
}

redisDbIF* RsCreateDbHandle(void)
{
    return createRedisDb();
}

void RsDestroyDbHandle(redisDbIF* db)
{
    if (db) {
        redisDb *redis_db = (redisDb*)db;
        closeRedisDb(redis_db);
    }
}

int RsFreeMemoryIfNeeded(redisDbIF* db)
{
    if (NULL == db) return REDIS_INVALID_ARG;

    redisDb *redis_db = (redisDb*)db;
    return freeMemoryIfNeeded(redis_db);
}

int RsActiveExpireCycle(redisDbIF *db)
{
    if (NULL == db) return REDIS_INVALID_ARG;

    redisDb *redis_db = (redisDb*)db;
    return activeExpireCycle(redis_db);
}

size_t RsGetUsedMemory(void)
{
    return zmalloc_used_memory();
}

void RsGetHitAndMissNum(long long *hits, long long *misses)
{
    atomicGet(g_db_status.stat_keyspace_hits, *hits);
    atomicGet(g_db_status.stat_keyspace_misses, *misses);
}

void RsResetHitAndMissNum(void)
{
    atomicSet(g_db_status.stat_keyspace_hits, 0);
    atomicSet(g_db_status.stat_keyspace_misses, 0);
}

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
int RsExpire(redisDbIF* db, robj *key, robj *expire)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return expireGenericCommand(redis_db, key, expire, mstime(), UNIT_SECONDS);
}

int RsExpireat(redisDbIF* db, robj *key, robj *expire)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return expireGenericCommand(redis_db, key, expire, 0, UNIT_SECONDS);
}

int RsTTL(redisDbIF* db, robj *key, int64_t *ttl)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }

    redisDb *redis_db = (redisDb*)db;
    if (NULL == lookupKeyRead(redis_db, key)) {
        *ttl = -2;
        return REDIS_KEY_NOT_EXIST;
    }

    long long expire = getExpire(redis_db, key);
    if (expire == -1) {
        *ttl = -1;
    } else {
        *ttl = (expire-mstime() < 0) ? -2 : (expire - mstime() + 500) / 1000;
    }

    return C_OK;
}

int RsPersist(redisDbIF *db, robj *key)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    if (NULL == lookupKeyWrite(redis_db,key)) {
        return REDIS_KEY_NOT_EXIST;
    }
    removeExpire(redis_db,key);

    return C_OK;
}

int RsType(redisDbIF *db, robj *key, sds *val)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    char *type;
    robj *o = lookupKeyRead(redis_db,key);
    if (o == NULL) {
        return REDIS_KEY_NOT_EXIST;
    } else {
        switch(o->type) {
        case OBJ_STRING: type = "string"; break;
        case OBJ_LIST: type = "list"; break;
        case OBJ_SET: type = "set"; break;
        case OBJ_ZSET: type = "zset"; break;
        case OBJ_HASH: type = "hash"; break;
        default: type = "unknown"; break;
        }
    }

    *val = sdsnewlen(type, strlen(type));

    return C_OK;
}

int RsDel(redisDbIF* db, robj *key)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    if (!dbDelete(redis_db, key)) {
        return REDIS_KEY_NOT_EXIST;
    }

    return C_OK;
}

int RsExists(redisDbIF* db, robj *key)
{
    if (NULL == db || NULL == key) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    return dbExists(redis_db, key);
}

int RsDbSize(redisDbIF* db, long long *dbsize)
{
    if (NULL == db) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    *dbsize = dictSize(redis_db->dict);

    return C_OK;
}

int RsFlushDb(redisDbIF *db)
{
    if (NULL == db) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    emptyDb(redis_db, NULL);

    return C_OK;
}

int RsRandomkey(redisDbIF *db, sds *key)
{
    if (NULL == db) {
        return REDIS_INVALID_ARG;
    }
    redisDb *redis_db = (redisDb*)db;

    robj *kobj;
    if ((kobj = dbRandomKey(redis_db)) == NULL) {
        return REDIS_NO_KEYS;
    }

    convertObjectToSds(kobj, key);
    
    decrRefCount(kobj);
    return C_OK;
}