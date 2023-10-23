//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/cache.h"

namespace cache {

Status RedisCache::ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  unsigned int items_size = score_members.size() * 2;
  robj **items = (robj **)zcallocate(sizeof(robj *) * items_size);
  for (unsigned int i = 0; i < score_members.size(); ++i) {
    items[i * 2] = createStringObjectFromLongDouble(score_members[i].score, 0);
    items[i * 2 + 1] =
        createObject(OBJ_STRING, sdsnewlen(score_members[i].member.data(), score_members[i].member.size()));
  }

  if (C_OK != RcZAdd(cache_, kobj, items, items_size)) {
    FreeObjectList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::Corruption("RcZAdd failed");
  }

  FreeObjectList(items, items_size);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZCard(std::string &key, unsigned long *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RcZCard(cache_, kobj, len))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RcZCard failed");
    }
  }

  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZCount(std::string &key, std::string &min, std::string &max, unsigned long *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZCount(cache_, kobj, minobj, maxobj, len))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZCount failed");
    }
  }

  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZIncrby(std::string &key, std::string &member, double increment) {
  if (C_OK != RcFreeMemoryIfNeeded(cache_)) {
    return Status::Corruption("[error] Free memory faild !");
  }

  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **items = (robj **)zcallocate(sizeof(robj *) * 2);
  items[0] = createStringObjectFromLongDouble(increment, 0);
  items[1] = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  if (C_OK != RcZIncrby(cache_, kobj, items, 2)) {
    FreeObjectList(items, 2);
    DecrObjectsRefCount(kobj);
    return Status::Corruption("RcZIncrby failed");
  }

  FreeObjectList(items, 2);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZRange(std::string &key, long start, long stop, std::vector<storage::ScoreMember> *score_members) {
  zitem *items = nullptr;
  unsigned long items_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RcZrange(cache_, kobj, start, stop, &items, &items_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RcZrange failed");
    }
  }

  for (unsigned long i = 0; i < items_size; ++i) {
    storage::ScoreMember sm;
    sm.score = items[i].score;
    sm.member.assign(items[i].member, sdslen(items[i].member));
    score_members->push_back(sm);
  }

  FreeZitemList(items, items_size);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZRangebyscore(std::string &key, std::string &min, std::string &max,
                                 std::vector<storage::ScoreMember> *score_members, int64_t offset, int64_t count) {
  zitem *items = nullptr;
  unsigned long items_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRangebyscore(cache_, kobj, minobj, maxobj, &items, &items_size, offset, count))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRangebyscore failed");
    }
  }

  for (unsigned long i = 0; i < items_size; ++i) {
    storage::ScoreMember sm;
    sm.score = items[i].score;
    sm.member.assign(items[i].member, sdslen(items[i].member));
    score_members->push_back(sm);
  }

  FreeZitemList(items, items_size);
  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRank(std::string &key, std::string &member, long *rank) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  if (C_OK != (ret = RcZRank(cache_, kobj, mobj, rank))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("member not exist");
    } else {
      DecrObjectsRefCount(kobj, mobj);
      return Status::Corruption("RcZRank failed");
    }
  }

  DecrObjectsRefCount(kobj, mobj);
  return Status::OK();
}

Status RedisCache::ZRem(std::string &key, std::vector<std::string> &members) {
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj **members_obj = (robj **)zcallocate(sizeof(robj *) * members.size());
  for (unsigned int i = 0; i < members.size(); ++i) {
    members_obj[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
  }

  int ret;
  if (C_OK != (ret = RcZRem(cache_, kobj, members_obj, members.size()))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      FreeObjectList(members_obj, members.size());
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      FreeObjectList(members_obj, members.size());
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RcZRem failed");
    }
  }

  FreeObjectList(members_obj, members.size());
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZRemrangebyrank(std::string &key, std::string &min, std::string &max) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRemrangebyrank(cache_, kobj, minobj, maxobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRemrangebyrank failed");
    }
  }

  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRemrangebyscore(std::string &key, std::string &min, std::string &max) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRemrangebyscore(cache_, kobj, minobj, maxobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRemrangebyscore failed");
    }
  }

  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRevrange(std::string &key, long start, long stop,
                             std::vector<storage::ScoreMember> *score_members) {
  zitem *items = nullptr;
  unsigned long items_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  if (C_OK != (ret = RcZRevrange(cache_, kobj, start, stop, &items, &items_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj);
      return Status::Corruption("RcZRevrange failed");
    }
  }

  for (unsigned long i = 0; i < items_size; ++i) {
    storage::ScoreMember sm;
    sm.score = items[i].score;
    sm.member.assign(items[i].member, sdslen(items[i].member));
    score_members->push_back(sm);
  }

  FreeZitemList(items, items_size);
  DecrObjectsRefCount(kobj);
  return Status::OK();
}

Status RedisCache::ZRevrangebyscore(std::string &key, std::string &min, std::string &max,
                                    std::vector<storage::ScoreMember> *score_members, int64_t offset, int64_t count) {
  zitem *items = nullptr;
  unsigned long items_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRevrangebyscore(cache_, kobj, minobj, maxobj, &items, &items_size, offset, count))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRevrangebyscore failed");
    }
  }

  for (unsigned long i = 0; i < items_size; ++i) {
    storage::ScoreMember sm;
    sm.score = items[i].score;
    sm.member.assign(items[i].member, sdslen(items[i].member));
    score_members->push_back(sm);
  }

  FreeZitemList(items, items_size);
  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRevrangebylex(std::string &key, std::string &min, std::string &max,
                                  std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRevrangebylex(cache_, kobj, minobj, maxobj, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRevrangebylex failed");
    }
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRevrank(std::string &key, std::string &member, long *rank) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  if (C_OK != (ret = RcZRevrank(cache_, kobj, mobj, rank))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("member not exist");
    } else {
      DecrObjectsRefCount(kobj, mobj);
      return Status::Corruption("RcZRevrank failed");
    }
  }

  DecrObjectsRefCount(kobj, mobj);
  return Status::OK();
}

Status RedisCache::ZScore(std::string &key, std::string &member, double *score) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
  if (C_OK != (ret = RcZScore(cache_, kobj, mobj, score))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("key not in cache");
    } else if (REDIS_ITEM_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, mobj);
      return Status::NotFound("member not exist");
    } else {
      DecrObjectsRefCount(kobj, mobj);
      return Status::Corruption("RcZScore failed");
    }
  }

  DecrObjectsRefCount(kobj, mobj);
  return Status::OK();
}

Status RedisCache::ZRangebylex(std::string &key, std::string &min, std::string &max,
                               std::vector<std::string> *members) {
  sds *vals = nullptr;
  unsigned long vals_size = 0;
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRangebylex(cache_, kobj, minobj, maxobj, &vals, &vals_size))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRangebylex failed");
    }
  }

  for (unsigned long i = 0; i < vals_size; ++i) {
    members->push_back(std::string(vals[i], sdslen(vals[i])));
  }

  FreeSdsList(vals, vals_size);
  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZLexcount(std::string &key, std::string &min, std::string &max, unsigned long *len) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZLexcount(cache_, kobj, minobj, maxobj, len))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZLexcount failed");
    }
  }

  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

Status RedisCache::ZRemrangebylex(std::string &key, std::string &min, std::string &max) {
  int ret;
  robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
  robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
  robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
  if (C_OK != (ret = RcZRemrangebylex(cache_, kobj, minobj, maxobj))) {
    if (REDIS_KEY_NOT_EXIST == ret) {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::NotFound("key not in cache");
    } else {
      DecrObjectsRefCount(kobj, minobj, maxobj);
      return Status::Corruption("RcZRemrangebylex failed");
    }
  }

  DecrObjectsRefCount(kobj, minobj, maxobj);
  return Status::OK();
}

}  // namespace cache
/* EOF */
