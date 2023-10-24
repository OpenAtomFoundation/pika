//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::LIndex(std::string &key, int64_t index, std::string *element) {
  sds val;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcLIndex(cache_, kobj, index, &val))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      return Status::NotFound("index not exist");
    }
    return Status::Corruption("RcLIndex failed");
  }

  element->clear();
  element->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::LInsert(std::string &key, storage::BeforeOrAfter &before_or_after, std::string &pivot,
                           std::string &value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  int where = (before_or_after == storage::Before) ? REDIS_LIST_HEAD : REDIS_LIST_TAIL;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *pobj = createObject(OBJ_STRING, sdsnewlen(pivot.data(), pivot.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, pobj, vobj);
  };
  if (C_OK != (ret = RcLInsert(cache_, kobj, where, pobj, vobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLInsert failed");
  }

  return Status::OK();
}

Status RedisCache::LLen(std::string &key, uint64_t *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcLLen(cache_, kobj, reinterpret_cast<unsigned long *>(len)))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLLen failed");
  }

  return Status::OK();
}

Status RedisCache::LPop(std::string &key, std::string *element) {
  sds val;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
        DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcLPop(cache_, kobj, &val))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLPop failed");
  }

  element->clear();
  element->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::LPush(std::string &key, std::vector<std::string> &values) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * values.size());
  for (unsigned int i = 0; i < values.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
  }
  DEFER {
    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != RcLPush(cache_, kobj, vals, values.size())) {
    return Status::Corruption("RcLPush failed");
  }

  return Status::OK();
}

Status RedisCache::LPushx(std::string &key, std::vector<std::string> &values) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * values.size());
  for (unsigned int i = 0; i < values.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
  }
  DEFER {
    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
  };
  int ret;
  if (C_OK != (ret = RcLPushx(cache_, kobj, vals, values.size()))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLPushx failed");
  }

  return Status::OK();
}

Status RedisCache::LRange(std::string &key, int64_t start, int64_t stop, std::vector<std::string> *values) {
  sds *vals = nullptr;
  uint64_t vals_size;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcLRange(cache_, kobj, start, stop, &vals, reinterpret_cast<unsigned long *>(&vals_size)))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLRange failed");
  }

  for (uint64_t i = 0; i < vals_size; ++i) {
    values->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  return Status::OK();
}

Status RedisCache::LRem(std::string &key, int64_t count, std::string &value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  if (C_OK != (ret = RcLRem(cache_, kobj, count, vobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcLRem failed");
  }

  return Status::OK();
}

Status RedisCache::LSet(std::string &key, int64_t index, std::string &value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
  DEFER {
    DecrObjectsRefCount(kobj, vobj);
  };
  if (C_OK != (ret = RcLSet(cache_, kobj, index, vobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      return Status::NotFound("item not exist");
    }
    return Status::Corruption("RcLSet failed");
  }

  return Status::OK();
}

Status RedisCache::LTrim(std::string &key, int64_t start, int64_t stop) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcLTrim(cache_, kobj, start, stop))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    } else {
      return Status::Corruption("RcLTrim failed");
    }
  }

  return Status::OK();
}

Status RedisCache::RPop(std::string &key, std::string *element) {
  sds val;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcRPop(cache_, kobj, &val))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcRPop failed");
  }

  element->clear();
  element->assign(val, sdslen(val));
  sdsfree(val);

  return Status::OK();
}

Status RedisCache::RPush(std::string &key, std::vector<std::string> &values) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * values.size());
  for (unsigned int i = 0; i < values.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
  }
  DEFER {
    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != RcRPush(cache_, kobj, vals, values.size())) {
    return Status::Corruption("RcRPush failed");
  }

  return Status::OK();
}

Status RedisCache::RPushx(std::string &key, std::vector<std::string> &values) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * values.size());
  for (unsigned int i = 0; i < values.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
  }
  DEFER {
    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
  };
  int ret;
  if (C_OK != (ret = RcRPushx(cache_, kobj, vals, values.size()))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcRPushx failed");
  }

  return Status::OK();
}

}  // namespace cache

/* EOF */