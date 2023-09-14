/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <ctype.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "db.h"
#include "object.h"
#include "atomicvar.h"
#include "commondef.h"
#include "commonfunc.h"
#include "zmalloc.h"

extern db_config g_db_config;
extern db_status g_db_status;


/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor           /* val destructor */
};

/* Generic hash table type where keys are Redis Objects, Values
 * dummy pointers. */
dictType objectKeyPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    NULL                       /* val destructor */
};

redisDb* createRedisDb(void)
{
    redisDb *db = zcalloc(sizeof(*db));
    if (NULL == db) return NULL;

    db->dict = dictCreate(&dbDictType, NULL);
    db->expires = dictCreate(&keyptrDictType, NULL);
    db->eviction_pool = evictionPoolAlloc();
    return db;
}

void closeRedisDb(redisDb *db)
{
    if (db) {
        dictRelease(db->dict);
        dictRelease(db->expires);
        evictionPoolDestroy(db->eviction_pool);
        zfree(db->eviction_pool);
        zfree(db);
    }
}

/* Update LFU when an object is accessed.
 * Firstly, decrement the counter if the decrement time is reached.
 * Then logarithmically increment the counter, and update the access time. */
void updateLFU(robj *val) {
    unsigned long counter = LFUDecrAndReturn(val);
    counter = LFULogIncr(counter);
    val->lru = (LFUGetTimeInMinutes()<<8) | counter;
}

/* Low level key lookup API, not actually called directly from commands
 * implementations that should instead rely on lookupKeyRead(),
 * lookupKeyWrite() and lookupKeyReadWithFlags(). */
robj *lookupKey(redisDb *db, robj *key, int flags) {
    dictEntry *de = dictFind(db->dict,key->ptr);
    if (de) {
        robj *val = dictGetVal(de);

        /* Update the access time for the ageing algorithm.
         * Don't do it if we have a saving child, as this will trigger
         * a copy on write madness. */
        int maxmemory_policy;
        atomicGet(g_db_config.maxmemory_policy, maxmemory_policy);
        if (!(flags & LOOKUP_NOTOUCH)) {
            if (maxmemory_policy & MAXMEMORY_FLAG_LFU) {
                updateLFU(val);
            } else {
                val->lru = LRU_CLOCK();
            }
        }
        return val;
    } else {
        return NULL;
    }
}

/* Lookup a key for read operations, or return NULL if the key is not found
 * in the specified DB.
 *
 * As a side effect of calling this function:
 * 1. A key gets expired if it reached it's TTL.
 * 2. The key last access time is updated.
 * 3. The global keys hits/misses stats are updated (reported in INFO).
 *
 * This API should not be used when we write to the key after obtaining
 * the object linked to the key, but only for read only operations.
 *
 * Flags change the behavior of this command:
 *
 *  LOOKUP_NONE (or zero): no special flags are passed.  默认的话，就是此key的最后访问时间会被修改
 *  LOOKUP_NOTOUCH: don't alter the last access time of the key. 不修改此key的最后访问时间

 *
 * Note: this function also returns NULL is the key is logically expired
 * but still existing, in case this is a slave, since this API is called only
 * for read operations. Even if the key expiry is master-driven, we can
 * correctly report a key is expired on slaves even if the master is lagging
 * expiring our key via DELs in the replication link. */
robj *lookupKeyReadWithFlags(redisDb *db, robj *key, int flags) {
    expireIfNeeded(db,key);
    robj *val = lookupKey(db,key,flags);
    if (val == NULL)
        atomicIncr(g_db_status.stat_keyspace_misses, 1);
    else
        atomicIncr(g_db_status.stat_keyspace_hits, 1);
    return val;
}

/* Like lookupKeyReadWithFlags(), but does not use any flag, which is the
 * common case. */
robj *lookupKeyRead(redisDb *db, robj *key) {
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
}

/* Lookup a key for write operations, and as a side effect, if needed, expires
 * the key if its TTL is reached.
 *
 * Returns the linked value object if the key exists or NULL if the key
 * does not exist in the specified DB. */
robj *lookupKeyWrite(redisDb *db, robj *key) {
    expireIfNeeded(db,key);
    return lookupKey(db,key,LOOKUP_NONE);
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
void dbAdd(redisDb *db, robj *key, robj *val) {
    sds copy = sdsdup(key->ptr);
    dictAdd(db->dict, copy, val);
 }

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. */
void dbOverwrite(redisDb *db, robj *key, robj *val) {
    dictEntry *de = dictFind(db->dict,key->ptr);

    int maxmemory_policy;
    atomicGet(g_db_config.maxmemory_policy, maxmemory_policy);
    if (maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        robj *old = dictGetVal(de);
        int saved_lru = old->lru;
        dictReplace(db->dict, key->ptr, val);
        val->lru = saved_lru;
        /* LFU should be not only copied but also updated
         * when a key is overwritten. */
        updateLFU(val);
    } else {
        dictReplace(db->dict, key->ptr, val);
    }
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent).
 *
 * All the new keys in the database should be craeted via this interface. */
void setKey(redisDb *db, robj *key, robj *val) {
    if (lookupKeyWrite(db,key) == NULL) {
        dbAdd(db,key,val);
    } else {
        dbOverwrite(db,key,val);
    }
    incrRefCount(val);
    removeExpire(db,key);
}

int dbExists(redisDb *db, robj *key) {
    return dictFind(db->dict,key->ptr) != NULL;
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
robj *dbRandomKey(redisDb *db) {
    dictEntry *de;
    // int maxtries = 100;
    // int allvolatile = dictSize(db->dict) == dictSize(db->expires);

    while(1) {
        sds key;
        robj *keyobj;

        de = dictGetRandomKey(db->dict);
        if (de == NULL) return NULL;

        key = dictGetKey(de);
        keyobj = createStringObject(key,sdslen(key));
        if (dictFind(db->expires,key)) {
            // if (allvolatile && server.masterhost && --maxtries == 0) {
            //      If the DB is composed only of keys with an expire set,
            //      * it could happen that all the keys are already logically
            //      * expired in the slave, so the function cannot stop because
            //      * expireIfNeeded() is false, nor it can stop because
            //      * dictGetRandomKey() returns NULL (there are keys to return).
            //      * To prevent the infinite loop we do some tries, but if there
            //      * are the conditions for an infinite loop, eventually we
            //      * return a key name that may be already expired. 
            //     return keyobj;
            // }
            if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                continue; /* search for another key. This expired. */
            }
        }
        return keyobj;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbDelete(redisDb *db, robj *key) {
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */
    if (dictSize(db->expires) > 0) dictDelete(db->expires,key->ptr);
    if (dictDelete(db->dict,key->ptr) == DICT_OK) {
        return 1;
    } else {
        return 0;
    }
}

/* Remove all keys from all the databases in a Redis server.
 * If callback is given the function is called from time to time to
 * signal that work is in progress.
 *
 * The dbnum can be -1 if all teh DBs should be flushed, or the specified
 * DB number if we want to flush only a single Redis database number.
 *
 * Flags are be EMPTYDB_NO_FLAGS if no special flags are specified or
 * EMPTYDB_ASYNC if we want the memory to be freed in a different thread
 * and the function to return ASAP.
 *
 * On success the fuction returns the number of keys removed from the
 * database(s). Otherwise -1 is returned in the specific case the
 * DB number is out of range, and errno is set to EINVAL. */
long long emptyDb(redisDb *db, void(callback)(void*)) {
    long long removed = 0;

    removed += dictSize(db->dict);
    dictEmpty(db->dict,callback);
    dictEmpty(db->expires,callback);
    atomicSet(g_db_status.stat_keyspace_hits, 0);
    atomicSet(g_db_status.stat_keyspace_misses, 0);

    return removed;
}

int removeExpire(redisDb *db, robj *key) {
    return dictDelete(db->expires,key->ptr) == DICT_OK;
}

/* Set an expire to the specified key. If the expire is set in the context
 * of an user calling a command 'c' is the client, otherwise 'c' is set
 * to NULL. The 'when' parameter is the absolute unix time in milliseconds
 * after which the key will no longer be considered valid. */
void setExpire(redisDb *db, robj *key, long long when) {
    dictEntry *kde, *de;

    /* Reuse the sds from the main dict in the expire dict */
    if (NULL != (kde = dictFind(db->dict,key->ptr))) {
        de = dictAddOrFind(db->expires,dictGetKey(kde));
        dictSetSignedIntegerVal(de,when); 
    }
}

/* This function is called when we are going to perform some operation
 * in a given key, but such key may be already logically expired even if
 * it still exists in the database. The main way this function is called
 * is via lookupKey*() family of functions.
 *
 * The behavior of the function depends on the replication role of the
 * instance, because slave instances do not expire keys, they wait
 * for DELs from the master for consistency matters. However even
 * slaves will try to have a coherent return value for the function,
 * so that read commands executed in the slave side will be able to
 * behave like if the key is expired even if still present (because the
 * master has yet to propagate the DEL).
 *
 * In masters as a side effect of finding a key which is expired, such
 * key will be evicted from the database. Also this may trigger the
 * propagation of a DEL/UNLINK command in AOF / replication stream.
 *
 * The return value of the function is 0 if the key is still valid,
 * otherwise the function returns 1 if the key is expired. */
int expireIfNeeded(redisDb *db, robj *key) {
    
    mstime_t when = getExpire(db,key);
    if (when < 0) return 0; /* No expire for this key */

    /* Return when this key has not expired */
    mstime_t now = mstime();
    if (now <= when) return 0;

    /* Delete the key */
    atomicIncr(g_db_status.stat_expiredkeys, 1);
    return dbDelete(db,key);
}

/* Return the expire time of the specified key, or -1 if no expire
 * is associated with this key (i.e. the key is non volatile) */
long long getExpire(redisDb *db, robj *key) {
    dictEntry *de;

    /* No expire? return ASAP */
    if (dictSize(db->expires) == 0 ||
       (de = dictFind(db->expires,key->ptr)) == NULL) return -1;

    return dictGetSignedIntegerVal(de);
}

/* ----------------------------------------------------------------------------
 * The external API for eviction: freeMemroyIfNeeded() is called by the
 * server when there is data to add in order to make space if needed.
 * --------------------------------------------------------------------------*/
int freeMemoryIfNeeded(redisDb *db) {
    size_t mem_used, mem_tofree, mem_freed;
    long long delta;
    unsigned long long maxmemory;
    int maxmemory_policy;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    atomicGet(g_db_config.maxmemory, maxmemory);
    mem_used = zmalloc_used_memory();
    if (mem_used <= maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    mem_tofree = mem_used - maxmemory;
    mem_freed = 0;

    atomicGet(g_db_config.maxmemory_policy, maxmemory_policy);
    if (maxmemory_policy == MAXMEMORY_NO_EVICTION) return C_ERR;

    while (mem_freed < mem_tofree) {
        int k, keys_freed = 0;
        sds bestkey = NULL;
        dict *dict;
        dictEntry *de;

        if (maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = db->eviction_pool;

            while(bestkey == NULL) {
                unsigned long keys = 0;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                dict = (maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                        db->dict : db->expires;
                if ((keys = dictSize(dict)) != 0) {
                    evictionPoolPopulate(dict, db->dict, pool);
                }
                if (!keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    if (pool[k].key == NULL) continue;

                    if (maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(db->dict, pool[k].key);
                    } else {
                        de = dictFind(db->expires, pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

        /* volatile-random and allkeys-random policy */
        else if (maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            dict = (maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                    db->dict : db->expires;
            if (dictSize(dict) != 0) {
                de = dictGetRandomKey(dict);
                bestkey = dictGetKey(de);
            }
        }

        /* Finally remove the selected key. */
        if (bestkey) {
            robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
            delta = (long long) zmalloc_used_memory();
            dbDelete(db,keyobj);
            delta -= (long long) zmalloc_used_memory();
            mem_freed += delta;

            g_db_status.stat_evictedkeys++;
            decrRefCount(keyobj);
            keys_freed++;
        }

        if (!keys_freed) return C_ERR;
    }

    return C_OK;
}

/* Helper function for the activeExpireCycle() function.
 * This function will try to expire the key that is stored in the hash table
 * entry 'de' of the 'expires' hash table of a Redis database.
 *
 * activeExpireCycle() 函数使用的检查键是否过期的辅佐函数。
 *
 * If the key is found to be expired, it is removed from the database and
 * 1 is returned. Otherwise no operation is performed and 0 is returned.
 *
 * 如果 de 中的键已经过期，那么移除它，并返回 1 ，否则不做动作，并返回 0 。
 *
 * When a key is expired, server.stat_expiredkeys is incremented.
 *
 * The parameter 'now' is the current time in milliseconds as is passed
 * to the function to avoid too many gettimeofday() syscalls.
 *
 * 参数 now 是毫秒格式的当前时间
 */
static int activeExpireCycleTryExpire(redisDb *db, dictEntry *de, long long now) {
    // 获取键的过期时间
    long long t = dictGetSignedIntegerVal(de);
    if (now > t) {

        // 键已过期
        sds key = dictGetKey(de);
        robj *keyobj = createStringObject(key,sdslen(key));

        // 从数据库中删除该键
        dbDelete(db,keyobj);
        decrRefCount(keyobj);
        // 更新计数器
        atomicIncr(g_db_status.stat_expiredkeys, 1);
        return 1;
    } else {
        return 0;
    }
}

int activeExpireCycle(redisDb *db)
{
    static int type = ACTIVE_EXPIRE_CYCLE_SLOW;

    unsigned long num, slots;

    // If there is nothing to expire
    if (0 == (num = dictSize(db->expires))) return 0;

    /* When there are less than 1% filled slots getting random
     * keys is expensive, so stop here waiting for better times...
     * The dictionary will be resized asap. */
    slots = dictSlots(db->expires);
    if (num && slots > DICT_HT_INITIAL_SIZE && (num*100/slots < 1)) return 0;

    if (ACTIVE_EXPIRE_CYCLE_SLOW == type) {
        num = (num > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP) ? ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP : num;
    }
    else {
        num = (num > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP * 2) ? ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP * 2 : num;
    }

    int expired = 0;
    long long now = mstime();
    while (num--) {
        dictEntry *de;

        // 从 expires 中随机取出一个带过期时间的键
        if ((de = dictGetRandomKey(db->expires)) == NULL) break;

        // 如果键已经过期，那么删除它，并将 expired 计数器增一
        if (activeExpireCycleTryExpire(db,de,now)) expired++;
    }

    // 如果已删除的过期键占当前总数据库带过期时间的键数量的 25 %
    // 則下次執行快速淘汰
    if (expired > ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP/4) {
        type = ACTIVE_EXPIRE_CYCLE_FAST;
    }
    else {
        type = ACTIVE_EXPIRE_CYCLE_SLOW;
    }

    return expired;
}

/* Prepare the string object stored at 'key' to be modified destructively
 * to implement commands like SETBIT or APPEND.
 *
 * An object is usually ready to be modified unless one of the two conditions
 * are true:
 *
 * 1) The object 'o' is shared (refcount > 1), we don't want to affect
 *    other users.
 * 2) The object encoding is not "RAW".
 *
 * If the object is found in one of the above conditions (or both) by the
 * function, an unshared / not-encoded copy of the string object is stored
 * at 'key' in the specified 'db'. Otherwise the object 'o' itself is
 * returned.
 *
 * USAGE:
 *
 * The object 'o' is what the caller already obtained by looking up 'key'
 * in 'db', the usage pattern looks like this:
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,OBJ_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * At this point the caller is ready to modify the object, for example
 * using an sdscat() call to append some data, or anything else.
 */
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
    assert(o->type == OBJ_STRING);
    if (o->refcount != 1 || o->encoding != OBJ_ENCODING_RAW) {
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(decoded->ptr, sdslen(decoded->ptr));
        decrRefCount(decoded);
        dbOverwrite(db,key,o);
    }
    return o;
}
