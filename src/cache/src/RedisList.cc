//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#include "cache/include/RedisCache.h"

namespace cache {

Status RedisCache::LIndex(std::string &key, long index, std::string *element) {
    sds val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsLIndex(m_RedisDB, kobj, index, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("index not exist");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLIndex failed");
        }
    }

    element->clear();
    element->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LInsert(std::string &key,
                    storage::BeforeOrAfter &before_or_after,
                    std::string &pivot,
                    std::string &value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    int where = (before_or_after == storage::Before) ? REDIS_LIST_HEAD : REDIS_LIST_TAIL;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *pobj = createObject(OBJ_STRING, sdsnewlen(pivot.data(), pivot.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != (ret = RsLInsert(m_RedisDB, kobj, where, pobj, vobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, pobj, vobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, pobj, vobj);
            return Status::Corruption("RsLInsert failed");
        }
    }

    DecrObjectsRefCount(kobj, pobj, vobj);
    return Status::OK();
}

Status RedisCache::LLen(std::string &key, unsigned long *len) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsLLen(m_RedisDB, kobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLLen failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LPop(std::string &key, std::string *element) {
    sds val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsLPop(m_RedisDB, kobj, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLPop failed");
        }
    }

    element->clear();
    element->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LPush(std::string &key, std::vector<std::string> &values) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **vals = (robj **)zcallocate(sizeof(robj*) * values.size());
    for (unsigned int i = 0; i < values.size(); ++i) {
        vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
    }

    if (C_OK != RsLPush(m_RedisDB, kobj, vals, values.size())) {
        FreeObjectList(vals, values.size());
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsLPush failed");
    }

    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LPushx(std::string &key, std::vector<std::string> &values) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **vals = (robj **)zcallocate(sizeof(robj*) * values.size());
    for (unsigned int i = 0; i < values.size(); ++i) {
        vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
    }

    int ret;
    if (C_OK != (ret = RsLPushx(m_RedisDB, kobj, vals, values.size()))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            FreeObjectList(vals, values.size());
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            FreeObjectList(vals, values.size());
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLPushx failed");
        }
    }

    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LRange(std::string &key, long start, long stop, std::vector<std::string> *values) {
    sds *vals = NULL;
    unsigned long vals_size;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsLRange(m_RedisDB, kobj, start, stop, &vals, &vals_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLRange failed");
        }
    }

    for (unsigned long i = 0; i < vals_size; ++i) {
        values->push_back(std::string(vals[i], sdslen(vals[i])));
    }

    FreeSdsList(vals, vals_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::LRem(std::string &key, long count, std::string &value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != (ret = RsLRem(m_RedisDB, kobj, count, vobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, vobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, vobj);
            return Status::Corruption("RsLRem failed");
        }
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::LSet(std::string &key, long index, std::string &value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != (ret = RsLSet(m_RedisDB, kobj, index, vobj))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, vobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, vobj);
            return Status::NotFound("item not exist");
        } else {
            DecrObjectsRefCount(kobj, vobj);
            return Status::Corruption("RsLSet failed");
        }
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::LTrim(std::string &key, long start, long stop) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsLTrim(m_RedisDB, kobj, start, stop))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsLTrim failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::RPop(std::string &key, std::string *element) {
    sds val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsRPop(m_RedisDB, kobj, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsRPop failed");
        }
    }

    element->clear();
    element->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::RPush(std::string &key, std::vector<std::string> &values) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **vals = (robj **)zcallocate(sizeof(robj*) * values.size());
    for (unsigned int i = 0; i < values.size(); ++i) {
        vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
    }

    if (C_OK != RsRPush(m_RedisDB, kobj, vals, values.size())) {
        FreeObjectList(vals, values.size());
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsRPush failed");
    }

    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::RPushx(std::string &key, std::vector<std::string> &values) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **vals = (robj **)zcallocate(sizeof(robj*) * values.size());
    for (unsigned int i = 0; i < values.size(); ++i) {
        vals[i] = createObject(OBJ_STRING, sdsnewlen(values[i].data(), values[i].size()));
    }

    int ret;
    if (C_OK != (ret = RsRPushx(m_RedisDB, kobj, vals, values.size()))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            FreeObjectList(vals, values.size());
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            FreeObjectList(vals, values.size());
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsRPushx failed");
        }
    }

    FreeObjectList(vals, values.size());
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

} // namespace dory

/* EOF */