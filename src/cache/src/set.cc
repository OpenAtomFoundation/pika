//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::SAdd(std::string &key, std::vector<std::string> &members) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * members.size());
  for (unsigned int i = 0; i < members.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
  }
  DEFER {
    FreeObjectList(vals, members.size());
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != RcSAdd(cache_, kobj, vals, members.size())) {
    return Status::Corruption("RcSAdd failed");
  }

  return Status::OK();
}

Status RedisCache::SCard(std::string &key, uint64_t *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcSCard(cache_, kobj, reinterpret_cast<unsigned long *>(len)))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcSCard failed");
  }

  return Status::OK();
}

Status RedisCache::SIsmember(std::string &key, std::string &member) {
  int ret, is_member;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  DEFER {
    DecrObjectsRefCount(kobj, mobj);
  };
  if (C_OK != (ret = RcSIsmember(cache_, kobj, mobj, &is_member))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("SIsmember failed");
  }

  return is_member ? Status::OK() : Status::NotFound("member not exist");
}

Status RedisCache::SMembers(std::string &key, std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcSMembers(cache_, kobj, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcSMembers failed");
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  return Status::OK();
}

Status RedisCache::SRem(std::string &key, std::vector<std::string> &members) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * members.size());
  for (unsigned int i = 0; i < members.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
  }
  DEFER {
    FreeObjectList(vals, members.size());
    DecrObjectsRefCount(kobj);
  };

  int ret;
  if (C_OK != (ret = RcSRem(cache_, kobj, vals, members.size()))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcSRem failed");
  }

  return Status::OK();
}

Status RedisCache::SRandmember(std::string &key, int64_t count, std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  if (C_OK != (ret = RcSRandmember(cache_, kobj, count, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcSRandmember failed");
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  return Status::OK();
}

}  // namespace cache

/* EOF */