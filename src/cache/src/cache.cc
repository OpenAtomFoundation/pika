// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include "cache/include/cache.h"
#include "pstd/include/pstd_string.h"
#include "pstd_defer.h"

namespace cache {

static int32_t GetRedisLRUPolicy(int32_t cache_lru_policy) {
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
  if (nullptr == cache_cfg || nullptr == db_cfg) {
    return;
  }

  db_cfg->maxmemory = cache_cfg->maxmemory;
  db_cfg->maxmemory_policy = GetRedisLRUPolicy(cache_cfg->maxmemory_policy);
  db_cfg->maxmemory_samples = cache_cfg->maxmemory_samples;
  db_cfg->lfu_decay_time = cache_cfg->lfu_decay_time;
}

RedisCache::RedisCache() {}

RedisCache::~RedisCache() {
  if (cache_) {
    RcDestroyCacheHandle(cache_);
    cache_ = nullptr;
  }
}

/*-----------------------------------------------------------------------------
 * Server APIs
 *----------------------------------------------------------------------------*/
void RedisCache::SetConfig(CacheConfig *cfg) {
  db_config db_cfg;
  ConvertCfg(cfg, &db_cfg);
  RcSetConfig(&db_cfg);
}

uint64_t RedisCache::GetUsedMemory(void) { return RcGetUsedMemory(); }

void RedisCache::GetHitAndMissNum(int64_t *hits, int64_t *misses) { RcGetHitAndMissNum((long long int*)hits, (long long int*)misses); }

void RedisCache::ResetHitAndMissNum(void) { RcResetHitAndMissNum(); }

Status RedisCache::Open(void) {
  cache_ = RcCreateCacheHandle();
  if (nullptr == cache_) {
    return Status::Corruption("RcCreateCacheHandle failed!");
  }

  return Status::OK();
}

int32_t RedisCache::ActiveExpireCycle(void) { return RcActiveExpireCycle(cache_); }

/*-----------------------------------------------------------------------------
 * Normal Commands
 *----------------------------------------------------------------------------*/
bool RedisCache::Exists(std::string& key) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    decrRefCount(kobj);
  };
  bool is_exist = RcExists(cache_, kobj);

  return is_exist;
}

int64_t RedisCache::DbSize(void) {
  int64_t dbsize = 0;
  RcCacheSize(cache_, (long long int*)&dbsize);
  return dbsize;
}

void RedisCache::FlushCache(void) { RcFlushCache(cache_); }

Status RedisCache::Del(const std::string& key) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    decrRefCount(kobj);
  };
  int ret = RcDel(cache_, kobj);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else {
      return Status::Corruption("RcDel failed");
    }
  }

  return Status::OK();
}

Status RedisCache::Expire(std::string& key, int64_t ttl) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, tobj);
  };
  int ret = RcExpire(cache_, kobj, tobj);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else {
      return Status::Corruption("RcExpire failed");
    }
  }

  return Status::OK();
}

Status RedisCache::Expireat(std::string& key, int64_t ttl) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, tobj);
  };
  int ret = RcExpireat(cache_, kobj, tobj);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcExpireat failed");
  }

  return Status::OK();
}

Status RedisCache::TTL(std::string& key, int64_t *ttl) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcTTL(cache_, kobj, ttl);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcTTL failed");
  }

  return Status::OK();
}

Status RedisCache::Persist(std::string& key) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcPersist(cache_, kobj);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcPersist failed");
  }

  return Status::OK();
}

Status RedisCache::Type(std::string& key, std::string *value) {
  sds val;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcType(cache_, kobj, &val);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcType failed");
  }

  value->clear();
  value->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::RandomKey(std::string *key) {
  sds val;
  int ret = RcRandomkey(cache_, &val);
  if (C_OK != ret) {
    if (REDIS_NO_KEYS == ret) {
      return Status::NotFound("no keys in cache");
    }
    return Status::Corruption("RcRandomkey failed");
  }

  key->clear();
  key->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

void RedisCache::DecrObjectsRefCount(robj *argv1, robj *argv2, robj *argv3) {
  if (nullptr != argv1) decrRefCount(argv1);
  if (nullptr != argv2) decrRefCount(argv2);
  if (nullptr != argv3) decrRefCount(argv3);
}

void RedisCache::FreeSdsList(sds *items, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    sdsfree(items[i]);
  }
  zfree(items);
}

void RedisCache::FreeObjectList(robj **items, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    decrRefCount(items[i]);
  }
  zfree(items);
}

void RedisCache::FreeHitemList(hitem *items, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    sdsfree(items[i].field);
    sdsfree(items[i].value);
  }
  zfree(items);
}

void RedisCache::FreeZitemList(zitem *items, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    sdsfree(items[i].member);
  }
  zfree(items);
}

void RedisCache::ConvertObjectToString(robj *obj, std::string *value) {
  if (sdsEncodedObject(obj)) {
    value->assign((char *)obj->ptr, sdslen((sds)obj->ptr));
  } else if (obj->encoding == OBJ_ENCODING_INT) {
    char buf[64];
    int len = pstd::ll2string(buf, 64, (long)obj->ptr);
    value->assign(buf, len);
  }
}

}  // namespace cache

/* EOF */