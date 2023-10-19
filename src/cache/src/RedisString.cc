//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <cstdlib>

#include "cache/include/RedisCache.h"

namespace cache {

Status RedisCache::Set(std::string &key, std::string &value, int64_t ttl) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    robj *tobj = createStringObjectFromLongLong(ttl);

    if (C_OK != RsSet(m_RedisDB, kobj, vobj, tobj)) {
        DecrObjectsRefCount(kobj, vobj, tobj);
        return Status::Corruption("RsSet failed");
    }

    DecrObjectsRefCount(kobj, vobj, tobj);
    return Status::OK();
}

Status RedisCache::SetWithoutTTL(std::string &key, std::string &value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));

    if (C_OK != RsSet(m_RedisDB, kobj, vobj, nullptr)) {
        DecrObjectsRefCount(kobj, vobj);
        return Status::Corruption("RsSetnx failed, key exists!");
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::Setnx(std::string &key, std::string &value, int64_t ttl) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    robj *tobj = createStringObjectFromLongLong(ttl);

    if (C_OK != RsSetnx(m_RedisDB, kobj, vobj, tobj)) {
        DecrObjectsRefCount(kobj, vobj, tobj);
        return Status::Corruption("RsSetnx failed, key exists!");
    }

    DecrObjectsRefCount(kobj, vobj, tobj);
    return Status::OK();
}

Status RedisCache::SetnxWithoutTTL(std::string &key, std::string &value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));

    if (C_OK != RsSetnx(m_RedisDB, kobj, vobj, nullptr)) {
        DecrObjectsRefCount(kobj, vobj);
        return Status::Corruption("RsSetnx failed, key exists!");
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::Setxx(std::string &key, std::string &value, int64_t ttl) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    robj *tobj = createStringObjectFromLongLong(ttl);

    if (C_OK != RsSetxx(m_RedisDB, kobj, vobj, tobj)) {
        DecrObjectsRefCount(kobj, vobj, tobj);
        return Status::Corruption("RsSetxx failed, key not exists!");
    }

    DecrObjectsRefCount(kobj, vobj, tobj);
    return Status::OK();
}

Status RedisCache::SetxxWithoutTTL(std::string &key, std::string &value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));

    if (C_OK != RsSetxx(m_RedisDB, kobj, vobj, nullptr)) {
        DecrObjectsRefCount(kobj, vobj);
        return Status::Corruption("RsSetxx failed, key not exists!");
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}
Status RedisCache::Get(const std::string &key, std::string* value) {
    robj *val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsGet(m_RedisDB, kobj, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsGet failed");
        }
    }

    value->clear();
    ConvertObjectToString(val, value);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Incr(std::string &key) {
    long long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsIncr(m_RedisDB, kobj, &ret)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsIncr failed");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Decr(std::string &key) {
    long long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsDecr(m_RedisDB, kobj, &ret)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsDecr failed!");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::IncrBy(std::string &key, long long incr) {
    long long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsIncrBy(m_RedisDB, kobj, incr, &ret)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsIncrBy failed!");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::DecrBy(std::string &key, long long incr) {
    long long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsDecrBy(m_RedisDB, kobj, incr, &ret)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsDecrBy failed!");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Incrbyfloat(std::string &key, long double incr) {
    long double ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsIncrByFloat(m_RedisDB, kobj, incr, &ret)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsIncrByFloat failed!");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::Append(std::string &key, std::string &value) {
    unsigned long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != RsAppend(m_RedisDB, kobj, vobj, &ret)) {
        DecrObjectsRefCount(kobj, vobj);
        return Status::Corruption("RsAppend failed!");
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::GetRange(std::string &key, int64_t start, int64_t end, std::string *value) {
    sds val;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsGetRange(m_RedisDB, kobj, start, end, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsGetRange failed");
        }
    }

    value->clear();
    value->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::SetRange(std::string &key, int64_t start, std::string &value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    unsigned long ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != RsSetRange(m_RedisDB, kobj, start, vobj, &ret)) {
        DecrObjectsRefCount(kobj, vobj);
        return Status::Corruption("SetRange failed!");
    }

    DecrObjectsRefCount(kobj, vobj);
    return Status::OK();
}

Status RedisCache::Strlen(std::string &key, int32_t *len) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsStrlen(m_RedisDB, kobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsStrlen failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

} // namespace dory

/* EOF */