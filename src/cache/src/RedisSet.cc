//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/RedisCache.h"

namespace cache {

Status RedisCache::SAdd(std::string &key, std::vector<std::string> &members) {
  if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * members.size());
  for (unsigned int i = 0; i < members.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
  }

  if (C_OK != RsSAdd(m_RedisDB, kobj, vals, members.size())) {
    FreeObjectList(vals, members.size());
    DecrObjectsRefCount(kobj);
    return Status::Corruption("RsSAdd failed");
  }

  FreeObjectList(vals, members.size());
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::SCard(std::string &key, unsigned long *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RsSCard(m_RedisDB, kobj, len))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RsSCard failed");
    }
  }

  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::SIsmember(std::string &key, std::string &member) {
  int ret, is_member;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  if (C_OK != (ret = RsSIsmember(m_RedisDB, kobj, mobj, &is_member))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, mobj);
      return Status::Corruption("SIsmember failed");
    }
  }

  DecrObjectsRefCount(kobj, mobj);
  return is_member ? Status::OK() : Status::NotFound("member not exist");
}

Status RedisCache::SMembers(std::string &key, std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RsSMembers(m_RedisDB, kobj, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RsSMembers failed");
    }
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::SRem(std::string &key, std::vector<std::string> &members) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **vals = (robj **)zcallocate(sizeof(robj *) * members.size());
  for (unsigned int i = 0; i < members.size(); ++i) {
    vals[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
  }

  int ret;
  if (C_OK != (ret = RsSRem(m_RedisDB, kobj, vals, members.size()))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      FreeObjectList(vals, members.size());
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      FreeObjectList(vals, members.size());
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RsSRem failed");
    }
  }

  FreeObjectList(vals, members.size());
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::SRandmember(std::string &key, long count, std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RsSRandmember(m_RedisDB, kobj, count, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RsSRandmember failed");
    }
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

}  // namespace cache

/* EOF */