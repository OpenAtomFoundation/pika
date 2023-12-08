// Copyright (c) 2023-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"
#include "pstd_defer.h"

namespace cache {

Status RedisCache::SAdd(std::string &key, std::vector<std::string> &members) {
  int ret = RcFreeMemoryIfNeeded(cache_);
  if (C_OK != ret) {
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
  int res = RcSAdd(cache_, kobj, vals, members.size());
  if (C_OK != res) {
    return Status::Corruption("RcSAdd failed");
  }

  return Status::OK();
}

Status RedisCache::SCard(std::string &key, uint64_t *len) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcSCard(cache_, kobj, reinterpret_cast<unsigned long *>(len));
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("RcSCard failed");
  }

  return Status::OK();
}

Status RedisCache::SIsmember(std::string &key, std::string &member) {
  int is_member = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  DEFER {
    DecrObjectsRefCount(kobj, mobj);
  };
  int ret = RcSIsmember(cache_, kobj, mobj, &is_member);
  if (C_OK != ret) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      return Status::NotFound("key not in cache");
    }
    return Status::Corruption("SIsmember failed");
  }

  return is_member ? Status::OK() : Status::NotFound("member not exist");
}

Status RedisCache::SMembers(std::string &key, std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size = 0;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcSMembers(cache_, kobj, &vals, &vals_size);
  if (C_OK != ret) {
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

  int ret = RcSRem(cache_, kobj, vals, members.size());
  if (C_OK != ret) {
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
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  DEFER {
    DecrObjectsRefCount(kobj);
  };
  int ret = RcSRandmember(cache_, kobj, count, &vals, &vals_size);
  if (C_OK != ret) {
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