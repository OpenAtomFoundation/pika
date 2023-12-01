// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <cstdlib>

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::Set(std::string &key, std::string &value, int64_t ttl) {
  int res = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != res) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };
  int ret = RcSet(cache_, kobj, vobj, tobj);
  if (C_OK != ret) {
    return Status::Corruption("RcSet failed");
  }

  return Status::OK();
}

Status RedisCache::SetWithoutTTL(std::string &key, std::string &value) {
  int ret = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != ret) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  int res = RcSet(cache_, kobj, vobj, nullptr);
  if (C_OK != res) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::Setnx(std::string &key, std::string &value, int64_t ttl) {
  int ret = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != ret) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };
  int res = RcSetnx(cache_, kobj, vobj, tobj);
  if (C_OK != res) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::SetnxWithoutTTL(std::string &key, std::string &value) {
  int res = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != res) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  int ret = RcSetnx(cache_, kobj, vobj, nullptr);
  if (C_OK != ret) {
    return Status::Corruption("RcSetnx failed, key exists!");
  }

  return Status::OK();
}

Status RedisCache::Setxx(std::string &key, std::string &value, int64_t ttl) {
  int ret = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != ret) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  robj *tobj = createStringObjectFromLongLong(ttl);
  DEFER {
    DecrObjectsRefCount(kobj, vobj, tobj);
  };
  int res = RcSetxx(cache_, kobj, vobj, tobj);
  if (C_OK != res) {
    return Status::Corruption("RcSetxx failed, key not exists!");
  }

  return Status::OK();
}

Status RedisCache::SetxxWithoutTTL(std::string &key, std::string &value) {
  int res = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != res) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  int ret = RcSetxx(cache_, kobj, vobj, nullptr);
  if (C_OK != ret) {
    return Status::Corruption("RcSetxx failed, key not exists!");
  }

  return Status::OK();
}

Status RedisCache::Get(const std::string &key, std::string *value) {
  robj *val;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcGet(cache_, kobj, &val);
  if (C_OK != ret) {
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
  int res = RcIncr(cache_, kobj, &ret);
  if (C_OK != res) {
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
  int res =  RcDecr(cache_, kobj, &ret);
  if (C_OK != res) {
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
  int res = RcIncrBy(cache_, kobj, incr, &ret);
  if (C_OK != res) {
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
  int res = RcDecrBy(cache_, kobj, incr, &ret);
  if (C_OK != res) {
    return Status::Corruption("RcDecrBy failed!");
  }

  return Status::OK();
}

Status RedisCache::Incrbyfloat(std::string &key, double incr) {
  long double ret = .0f;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int res = RcIncrByFloat(cache_, kobj, incr, &ret);
  if (C_OK != res) {
    return Status::Corruption("RcIncrByFloat failed!");
  }

  return Status::OK();
}

Status RedisCache::Append(std::string &key, std::string &value) {
  uint64_t ret = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  int res = RcAppend(cache_, kobj, vobj, reinterpret_cast<unsigned long *>(&ret));
  if (C_OK != res) {
    return Status::Corruption("RcAppend failed!");
  }

  return Status::OK();
}

Status RedisCache::GetRange(std::string &key, int64_t start, int64_t end, std::string *value) {
  sds val;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcGetRange(cache_, kobj, start, end, &val);
  if (C_OK != ret) {
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

  uint64_t ret = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  int res = RcSetRange(cache_, kobj, start, vobj, reinterpret_cast<unsigned long *>(&ret));
  if (C_OK != res) {
    return Status::Corruption("SetRange failed!");
  }

  return Status::OK();
}

Status RedisCache::Strlen(std::string &key, int32_t *len) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcStrlen(cache_, kobj, len);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcStrlen failed");
  }

  return Status::OK();
}

}  // namespace cache

/* EOF */