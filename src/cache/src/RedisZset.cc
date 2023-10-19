//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "cache/include/RedisCache.h"

namespace cache {

Status RedisCache::ZAdd(std::string &key, std::vector<storage::ScoreMember> &score_members) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    unsigned int items_size = score_members.size() * 2;
    robj **items = (robj **)zcallocate(sizeof(robj*) * items_size);
    for (unsigned int i = 0; i < score_members.size(); ++i) {
        items[i*2] = createStringObjectFromLongDouble(score_members[i].score, 0);
        items[i*2+1] = createObject(OBJ_STRING, sdsnewlen(score_members[i].member.data(), score_members[i].member.size()));
    }

    if (C_OK != RsZAdd(m_RedisDB, kobj, items, items_size)) {
        FreeObjectList(items, items_size);
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsZAdd failed");
    }

    FreeObjectList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::ZCard(std::string &key, unsigned long *len) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsZCard(m_RedisDB, kobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsZCard failed");
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
    if (C_OK != (ret = RsZCount(m_RedisDB, kobj, minobj, maxobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZCount failed");
        }
    }

    DecrObjectsRefCount(kobj, minobj, maxobj);
    return Status::OK();
}

Status RedisCache::ZIncrby(std::string &key, std::string &member, double increment) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **items = (robj **)zcallocate(sizeof(robj*) * 2);
    items[0] = createStringObjectFromLongDouble(increment, 0);
    items[1] = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
    if (C_OK != RsZIncrby(m_RedisDB, kobj, items, 2)) {
        FreeObjectList(items, 2);
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsZIncrby failed");
    }

    FreeObjectList(items, 2);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::ZRange(std::string &key,
                  long start, long stop,
                  std::vector<storage::ScoreMember> *score_members) {
    zitem *items = NULL;
    unsigned long items_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsZrange(m_RedisDB, kobj, start, stop, &items, &items_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsZrange failed");
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

Status RedisCache::ZRangebyscore(std::string &key,
                          std::string &min, std::string &max,
                          std::vector<storage::ScoreMember> *score_members,
                          int64_t offset, int64_t count) {
    zitem *items = NULL;
    unsigned long items_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
    robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
    if (C_OK != (ret = RsZRangebyscore(m_RedisDB, kobj, minobj, maxobj, &items, &items_size, offset, count))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRangebyscore failed");
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
    if (C_OK != (ret = RsZRank(m_RedisDB, kobj, mobj, rank))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("member not exist");
        } else {
            DecrObjectsRefCount(kobj, mobj);
            return Status::Corruption("RsZRank failed");
        }
    }

    DecrObjectsRefCount(kobj, mobj);
    return Status::OK();
}

Status RedisCache::ZRem(std::string &key, std::vector<std::string> &members) {
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **members_obj = (robj **)zcallocate(sizeof(robj*) * members.size());
    for (unsigned int i = 0; i < members.size(); ++i) {
        members_obj[i] = createObject(OBJ_STRING, sdsnewlen(members[i].data(), members[i].size()));
    }

    int ret;
    if (C_OK != (ret = RsZRem(m_RedisDB, kobj, members_obj, members.size()))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            FreeObjectList(members_obj, members.size());
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            FreeObjectList(members_obj, members.size());
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsZRem failed");
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
    if (C_OK != (ret = RsZRemrangebyrank(m_RedisDB, kobj, minobj, maxobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRemrangebyrank failed");
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
    if (C_OK != (ret = RsZRemrangebyscore(m_RedisDB, kobj, minobj, maxobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRemrangebyscore failed");
        }
    }

    DecrObjectsRefCount(kobj, minobj, maxobj);
    return Status::OK();
}

Status RedisCache::ZRevrange(std::string &key,
                      long start, long stop,
                      std::vector<storage::ScoreMember> *score_members) {
    zitem *items = NULL;
    unsigned long items_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsZRevrange(m_RedisDB, kobj, start, stop, &items, &items_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsZRevrange failed");
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

Status RedisCache::ZRevrangebyscore(std::string &key,
                             std::string &min, std::string &max,
                             std::vector<storage::ScoreMember> *score_members,
                             int64_t offset, int64_t count) {
    zitem *items = NULL;
    unsigned long items_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
    robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
    if (C_OK != (ret = RsZRevrangebyscore(m_RedisDB, kobj, minobj, maxobj, &items, &items_size, offset, count))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRevrangebyscore failed");
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

Status RedisCache::ZRevrangebylex(std::string &key,
                           std::string &min, std::string &max,
                           std::vector<std::string> *members) {
    sds *vals = NULL;
    unsigned long vals_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
    robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
    if (C_OK != (ret = RsZRevrangebylex(m_RedisDB, kobj, minobj, maxobj, &vals, &vals_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRevrangebylex failed");
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
    if (C_OK != (ret = RsZRevrank(m_RedisDB, kobj, mobj, rank))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("member not exist");
        } else {
            DecrObjectsRefCount(kobj, mobj);
            return Status::Corruption("RsZRevrank failed");
        }
    }

    DecrObjectsRefCount(kobj, mobj);
    return Status::OK();
}

Status RedisCache::ZScore(std::string &key, std::string &member, double *score) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *mobj = createObject(OBJ_STRING, sdsnewlen(member.data(), member.size()));
    if (C_OK != (ret = RsZScore(m_RedisDB, kobj, mobj, score))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, mobj);
            return Status::NotFound("member not exist");
        } else {
            DecrObjectsRefCount(kobj, mobj);
            return Status::Corruption("RsZScore failed");
        }
    }

    DecrObjectsRefCount(kobj, mobj);
    return Status::OK();
}

Status RedisCache::ZRangebylex(std::string &key,
                        std::string &min, std::string &max,
                        std::vector<std::string> *members) {
    sds *vals = NULL;
    unsigned long vals_size = 0;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *minobj = createObject(OBJ_STRING, sdsnewlen(min.data(), min.size()));
    robj *maxobj = createObject(OBJ_STRING, sdsnewlen(max.data(), max.size()));
    if (C_OK != (ret = RsZRangebylex(m_RedisDB, kobj, minobj, maxobj, &vals, &vals_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRangebylex failed");
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
    if (C_OK != (ret = RsZLexcount(m_RedisDB, kobj, minobj, maxobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZLexcount failed");
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
    if (C_OK != (ret = RsZRemrangebylex(m_RedisDB, kobj, minobj, maxobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, minobj, maxobj);
            return Status::Corruption("RsZRemrangebylex failed");
        }
    }

    DecrObjectsRefCount(kobj, minobj, maxobj);
    return Status::OK();
}

} // namespace dory

/* EOF */
