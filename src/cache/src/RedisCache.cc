//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include <cstdlib>
#include <cstring>
#include "cache/include/RedisCache.h"
#include "pstd/include/pstd_string.h"

namespace cache {

static int GetRedisLRUPolicy(int cache_lru_policy) {
    switch (cache_lru_policy) {
        case CACHE_VOLATILE_LRU:
            return MAXMEMORY_VOLATILE_LRU;
        case CACHE_ALLKEYS_LRU:
            return MAXMEMORY_ALLKEYS_LRU;
        case CACHE_VOLATILE_LFU:
            return MAXMEMORY_VOLATILE_LFU;
        case CACHE_ALLKEYS_LFU:
            return MAXMEMORY_ALLKEYS_LFU;
        case CACHE_VOLATILE_RANDOM:
            return MAXMEMORY_VOLATILE_RANDOM;
        case CACHE_ALLKEYS_RANDOM:
            return MAXMEMORY_ALLKEYS_RANDOM;
        case CACHE_VOLATILE_TTL:
            return MAXMEMORY_VOLATILE_TTL;
        case CACHE_NO_EVICTION:
            return MAXMEMORY_NO_EVICTION;
        default:
            return MAXMEMORY_NO_EVICTION;
    }
}

static void ConvertCfg(CacheConfig *cache_cfg, db_config *db_cfg) {
    if (NULL == cache_cfg || NULL == db_cfg) {
        return;
    }

    db_cfg->maxmemory = cache_cfg->maxmemory;
    db_cfg->maxmemory_policy = GetRedisLRUPolicy(cache_cfg->maxmemory_policy);
    db_cfg->maxmemory_samples = cache_cfg->maxmemory_samples;
    db_cfg->lfu_decay_time = cache_cfg->lfu_decay_time;
}

RedisCache::RedisCache() {}

RedisCache::~RedisCache() {
    if (m_RedisDB) {
        RsDestroyDbHandle(m_RedisDB);
        m_RedisDB = NULL;
    }
}

/*-----------------------------------------------------------------------------
 * Server APIs
 *----------------------------------------------------------------------------*/
void RedisCache::SetConfig(CacheConfig *cfg) {
    db_config db_cfg;
    ConvertCfg(cfg, &db_cfg);
    RsSetConfig(&db_cfg);
}

uint64_t RedisCache::GetUsedMemory(void) {
    return RsGetUsedMemory();
}

void RedisCache::GetHitAndMissNum(long long *hits, long long *misses) {
    RsGetHitAndMissNum(hits, misses);
}

void RedisCache::ResetHitAndMissNum(void) {
    RsResetHitAndMissNum();
}

Status RedisCache::Open(void) {
    m_RedisDB = RsCreateDbHandle();
    if (NULL == m_RedisDB) {
        return Status::Corruption("RsCreateDbHandle failed!");
    }

    return Status::OK();
}

int RedisCache::ActiveExpireCycle(void) {
    return RsActiveExpireCycle(m_RedisDB);
}

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
bool RedisCache::Exists(std::string &key) {
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    bool is_exist = RsExists(m_RedisDB, kobj);

    decrRefCount(kobj);
    return is_exist;
}

long long RedisCache::DbSize(void) {
    long long dbsize = 0;
    RsDbSize(m_RedisDB, &dbsize);
    return dbsize;
}

void RedisCache::FlushDb(void) {
    RsFlushDb(m_RedisDB);
}

Status RedisCache::Del(const std::string &key) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsDel(m_RedisDB, kobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsDel failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Expire(std::string &key, int64_t ttl) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *tobj = createStringObjectFromLongLong(ttl);
    if (C_OK != (ret = RsExpire(m_RedisDB, kobj, tobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, tobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, tobj);
            return Status::Corruption("RsExpire failed");
        }
    }

    DecrObjectsRefCount(kobj, tobj);
    return Status::OK();
}

Status RedisCache::Expireat(std::string &key, int64_t ttl) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *tobj = createStringObjectFromLongLong(ttl);
    if (C_OK != (ret = RsExpireat(m_RedisDB, kobj, tobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, tobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, tobj);
            return Status::Corruption("RsExpireat failed");
        }
    }

    DecrObjectsRefCount(kobj, tobj);
    return Status::OK();
}

Status RedisCache::TTL(std::string &key, int64_t *ttl) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsTTL(m_RedisDB, kobj, ttl))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsTTL failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Persist(std::string &key) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsPersist(m_RedisDB, kobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsPersist failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Type(std::string &key, std::string *value) {
    sds val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsType(m_RedisDB, kobj, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsType failed");
        }
    }

    value->clear();
    value->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::RandomKey(std::string *key) {
    sds val;
    int ret;
    if (C_OK != (ret = RsRandomkey(m_RedisDB, &val))) {
        if (REDIS_NO_KEYS == ret) {
            return Status::NotFound("no keys in cache");
        } else {
            return Status::Corruption("RsRandomkey failed");
        }
    }

    key->clear();
    key->assign(val, sdslen(val));
    sdsfree(val);

    return Status::OK();
}

void RedisCache::DecrObjectsRefCount(robj *argv1, robj *argv2, robj *argv3) {
    if (NULL != argv1) decrRefCount(argv1);
    if (NULL != argv2) decrRefCount(argv2);
    if (NULL != argv3) decrRefCount(argv3);
}

void RedisCache::FreeSdsList(sds *items, unsigned int size) {
    unsigned int i;
    for (i = 0; i < size; ++i) {
        sdsfree(items[i]);
    }
    zfree(items);
}

void RedisCache::FreeObjectList(robj **items, unsigned int size) {
    unsigned int i;
    for (i = 0; i < size; ++i) {
        decrRefCount(items[i]);
    }
    zfree(items);
}

void RedisCache::FreeHitemList(hitem *items, unsigned int size) {
    unsigned int i;
    for (i = 0; i < size; ++i) {
        sdsfree(items[i].field);
        sdsfree(items[i].value);
    }
    zfree(items);
}

void RedisCache::FreeZitemList(zitem *items, unsigned int size) {
    unsigned int i;
    for (i = 0; i < size; ++i) {
        sdsfree(items[i].member);
    }
    zfree(items);
}

void RedisCache::ConvertObjectToString(robj *obj, std::string *value) {
    if (sdsEncodedObject(obj)) {
        value->assign((char*)obj->ptr, sdslen((sds)obj->ptr));
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        char buf[64];
        int len = pstd::ll2string(buf,64,(long)obj->ptr);;
        value->assign(buf, len);
    }
}

} // namespace dory

/* EOF */