//  Copyright (c) 2023-present The dory Authors.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/include/RedisCache.h"

namespace cache {

Status RedisCache::SetBit(std::string &key, size_t offset, long value) {
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != RsSetBit(m_RedisDB, kobj, offset, value)) {
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsSetBit failed");
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::GetBit(std::string &key, size_t offset, long *value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsGetBit(m_RedisDB, kobj, offset, value))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsGetBit failed");
        }
    }
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::BitCount(std::string &key, long start, long end, long *value, bool have_offset) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsBitCount(m_RedisDB, kobj, start, end, value, have_offset))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsBitCount failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::BitPos(std::string &key, long bit, long *value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsBitPos(m_RedisDB, kobj, bit, -1, -1, value, BIT_POS_NO_OFFSET))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsBitPos failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::BitPos(std::string &key, long bit, long start, long *value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsBitPos(m_RedisDB, kobj, bit, start, -1, value, BIT_POS_START_OFFSET))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsBitPos failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status RedisCache::BitPos(std::string &key, long bit, long start, long end, long *value) {
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsBitPos(m_RedisDB, kobj, bit, start, end, value, BIT_POS_START_END_OFFSET))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsBitPos failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

} // namespace dory

/* EOF */
