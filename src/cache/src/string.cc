//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <cstdlib>

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::Set(std::string &key, std::string &value, int64_t ttl) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };

  if (C_OK != RcSet(cache_, kobj, vobj, tobj)) {
    return Status::Corruption("RcSet failed");
  }

  return Status::OK();
}

Status RedisCache::SetWithoutTTL(std::string &key, std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };

  if (C_OK != RcSet(cache_, kobj, vobj, nullptr)) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::Setnx(std::string &key, std::string &value, int64_t ttl) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };

  if (C_OK != RcSetnx(cache_, kobj, vobj, tobj)) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::SetnxWithoutTTL(std::string &key, std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };

  if (C_OK != RcSetnx(cache_, kobj, vobj, nullptr)) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::Setxx(std::string &key, std::string &value, int64_t ttl) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };

  if (C_OK != RcSetxx(cache_, kobj, vobj, tobj)) {
    return Status::Corruption("RcSetxx failed, key not exists!");
  }

  return Status::OK();
}

Status RedisCache::SetxxWithoutTTL(std::string &key, std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };

  if (C_OK != RcSetxx(cache_, kobj, vobj, nullptr)) {
    return Status::Corruption("RcSetxx failed, key not exists!");
  }

  return Status::OK();
}

Status RedisCache::Get(const std::string &key, std::string *value) {
  robj *val;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcGet(cache_, kobj, &val))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else {
      return Status::Corruption("RcGet failed");
    }
  }

  value->clear();
  ConvertObjectToString(val, value);

  return Status::OK();
}

Status RedisCache::Incr(std::string &key) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  long long int ret;
  if (C_OK != RcIncr(cache_, kobj, &ret)) {
    return Status::Corruption("RcIncr failed");
  }

  return Status::OK();
}

Status RedisCache::Decr(std::string &key) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  long long int ret;
  if (C_OK != RcDecr(cache_, kobj, &ret)) {
    return Status::Corruption("RcDecr failed!");
  }

  return Status::OK();
}

Status RedisCache::IncrBy(std::string &key, int64_t incr) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  long long int ret;
  if (C_OK != RcIncrBy(cache_, kobj, incr, &ret)) {
    return Status::Corruption("RcIncrBy failed!");
  }

  return Status::OK();
}

Status RedisCache::DecrBy(std::string &key, int64_t incr) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  long long int ret;
  if (C_OK != RcDecrBy(cache_, kobj, incr, &ret)) {
    return Status::Corruption("RcDecrBy failed!");
  }

  return Status::OK();
}

Status RedisCache::Incrbyfloat(std::string &key, double incr) {
  long double ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != RcIncrByFloat(cache_, kobj, incr, &ret)) {
    return Status::Corruption("RcIncrByFloat failed!");
  }

  return Status::OK();
}

Status RedisCache::Append(std::string &key, std::string &value) {
  uint64_t ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  if (C_OK != RcAppend(cache_, kobj, vobj, reinterpret_cast<unsigned long *>(&ret))) {
    return Status::Corruption("RcAppend failed!");
  }

  return Status::OK();
}

Status RedisCache::GetRange(std::string &key, int64_t start, int64_t end, std::string *value) {
  sds val;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcGetRange(cache_, kobj, start, end, &val))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else {
      return Status::Corruption("RcGetRange failed");
    }
  }

  value->clear();
  value->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::SetRange(std::string &key, int64_t start, std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  uint64_t ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  if (C_OK != RcSetRange(cache_, kobj, start, vobj, reinterpret_cast<unsigned long *>(&ret))) {
    return Status::Corruption("SetRange failed!");
  }

  return Status::OK();
}

Status RedisCache::Strlen(std::string &key, int32_t *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcStrlen(cache_, kobj, len))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcStrlen failed");
  }

  return Status::OK();
}

}  // namespace cache

/* EOF */