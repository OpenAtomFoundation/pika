//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"

#include "pstd_defer.h"

namespace cache {

Status RedisCache::SetBit(std::string &key, size_t offset, int64_t value) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  // createObject is a function in redis, the init ref count of robj is 1
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
        DecrObjectsRefCount(kobj);
  };

  if (C_OK != RcSetBit(cache_, kobj, offset, value)) {
    return Status::Corruption("RcSetBit failed");
  }

  return Status::OK();
}

Status RedisCache::GetBit(std::string &key, size_t offset, int64_t *value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != (ret = RcGetBit(cache_, kobj, offset, (long*)value))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }

    return Status::Corruption("RcGetBit failed");
  }

  return Status::OK();
}

Status RedisCache::BitCount(std::string &key, int64_t start, int64_t end, int64_t *value, bool have_offset) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != (ret = RcBitCount(cache_, kobj, start, end, (long*)value, (int)have_offset))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }

    return Status::Corruption("RcBitCount failed");
  }

  return Status::OK();
}

Status RedisCache::BitPos(std::string &key, int64_t bit, int64_t *value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != (ret = RcBitPos(cache_, kobj, bit, -1, -1, (long*)value, BIT_POS_NO_OFFSET))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcBitPos failed");
  }

  return Status::OK();
}

Status RedisCache::BitPos(std::string &key, int64_t bit, int64_t start, int64_t *value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != (ret = RcBitPos(cache_, kobj, bit, start, -1, (long*)value, BIT_POS_START_OFFSET))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcBitPos failed");
  }

  return Status::OK();
}

Status RedisCache::BitPos(std::string &key, int64_t bit, int64_t start, int64_t end, int64_t *value) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };

  if (C_OK != (ret = RcBitPos(cache_, kobj, bit, start, end, (long*)value, BIT_POS_START_END_OFFSET))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcBitPos failed");
  }

  return Status::OK();
}

} // namespace cache

/* EOF */
