#include <cstdlib>
#include <cstring>

#include "RedisCache.h"

namespace dory {

Status
RedisCache::HDel(std::string &key, std::vector<std::string> &fields)
{
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj **fields_obj = (robj **)zcalloc(sizeof(robj*) * fields.size());
    for (unsigned int i = 0; i < fields.size(); ++i) {
        fields_obj[i] = createObject(OBJ_STRING, sdsnewlen(fields[i].data(), fields[i].size()));
    }

    int ret;
    unsigned long deleted;
    if (C_OK != (ret = RsHDel(m_RedisDB, kobj, fields_obj, fields.size(), &deleted))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            FreeObjectList(fields_obj, fields.size());
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            FreeObjectList(fields_obj, fields.size());
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj);
    FreeObjectList(fields_obj, fields.size());
    return Status::OK();
}

Status
RedisCache::HSet(std::string &key, std::string &field, std::string &value)
{
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != RsHSet(m_RedisDB, kobj, fobj, vobj)) {
        DecrObjectsRefCount(kobj, fobj, vobj);
        return Status::Corruption("RsHSet failed");
    }

    DecrObjectsRefCount(kobj, fobj, vobj);
    return Status::OK();
}

Status
RedisCache::HSetnx(std::string &key, std::string &field, std::string &value)
{
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    robj *vobj = createObject(OBJ_STRING, sdsnewlen(value.data(), value.size()));
    if (C_OK != RsHSetnx(m_RedisDB, kobj, fobj, vobj)) {
        DecrObjectsRefCount(kobj, fobj, vobj);
        return Status::Corruption("RsHSetnx failed");
    }

    DecrObjectsRefCount(kobj, fobj, vobj);
    return Status::OK();
}

Status
RedisCache::HMSet(std::string &key, std::vector<storage::FieldValue> &fvs)
{
    if (C_OK != RsFreeMemoryIfNeeded(m_RedisDB)) {
        return Status::Corruption("[error] Free memory faild !");
    }

    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    unsigned int items_size = fvs.size() * 2;
    robj **items = (robj **)zcalloc(sizeof(robj*) * items_size);
    for (unsigned int i = 0; i < fvs.size(); ++i) {
        items[i*2] = createObject(OBJ_STRING, sdsnewlen(fvs[i].field.data(), fvs[i].field.size()));
        items[i*2+1] = createObject(OBJ_STRING, sdsnewlen(fvs[i].value.data(), fvs[i].value.size()));
    }

    if (C_OK != RsHMSet(m_RedisDB, kobj, items, items_size)) {
        FreeObjectList(items, items_size);
        DecrObjectsRefCount(kobj);
        return Status::Corruption("RsHMSet failed");
    }

    FreeObjectList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HGet(std::string &key, std::string &field, std::string *value)
{
    int ret;
    sds val;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    if (C_OK != (ret = RsHGet(m_RedisDB, kobj, fobj, &val))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            return Status::NotFound("key not in cache");
        } else if (REDIS_ITEM_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            // todo(leehao): its better to let pstd::Status to inherit rocksdb::Status
            return Status::NotFound("field not exist");
        } else {
            DecrObjectsRefCount(kobj, fobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    value->clear();
    value->assign(val, sdslen(val));
    sdsfree(val);

    DecrObjectsRefCount(kobj, fobj);
    return Status::OK();
}

Status
RedisCache::HMGet(std::string &key,
                  std::vector<std::string> &fields,
                  std::vector<storage::ValueStatus>* vss)
{
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    hitem *items = (hitem *)zcalloc(sizeof(hitem) * fields.size());
    for (unsigned int i = 0; i < fields.size(); ++i) {
        items[i].field = sdsnewlen(fields[i].data(), fields[i].size());
    }

    int ret;
    if (C_OK != (ret = RsHMGet(m_RedisDB, kobj, items, fields.size()))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            FreeHitemList(items, fields.size());
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            FreeHitemList(items, fields.size());
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    vss->clear();
    for (unsigned int i = 0; i < fields.size(); ++i) {
        if (C_OK == items[i].status) {
            vss->push_back({std::string(items[i].value, sdslen(items[i].value)), rocksdb::Status::OK()});
        } else {
            vss->push_back({std::string(), rocksdb::Status::NotFound()});
        }
    }

    FreeHitemList(items, fields.size());
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HGetall(std::string &key, std::vector<storage::FieldValue> *fvs)
{
    hitem *items;
    unsigned long items_size;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsHGetAll(m_RedisDB, kobj, &items, &items_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    for (unsigned long i = 0; i < items_size; ++i) {
        storage::FieldValue fv;
        fv.field.assign(items[i].field, sdslen(items[i].field));
        fv.value.assign(items[i].value, sdslen(items[i].value));
        fvs->push_back(fv);
    }

    FreeHitemList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HKeys(std::string &key, std::vector<std::string> *fields)
{
    hitem *items;
    unsigned long items_size;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsHKeys(m_RedisDB, kobj, &items, &items_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    for (unsigned long i = 0; i < items_size; ++i) {
        fields->push_back(std::string(items[i].field, sdslen(items[i].field)));
    }

    FreeHitemList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HVals(std::string &key, std::vector<std::string> *values)
{
    hitem *items;
    unsigned long items_size;
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsHVals(m_RedisDB, kobj, &items, &items_size))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    for (unsigned long i = 0; i < items_size; ++i) {
        values->push_back(std::string(items[i].value, sdslen(items[i].value)));
    }

    FreeHitemList(items, items_size);
    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HExists(std::string &key, std::string &field)
{
    int ret, is_exist;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    if (C_OK != (ret = RsHExists(m_RedisDB, kobj, fobj, &is_exist))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, fobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj, fobj);
    return is_exist ? Status::OK() : Status::NotFound("field not exist");
}

Status
RedisCache::HIncrby(std::string &key, std::string &field, int64_t value)
{
    int ret;
    long long result;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    if (C_OK != (ret = RsHIncrby(m_RedisDB, kobj, fobj, value, &result))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, fobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj, fobj);
    return Status::OK();
}

Status
RedisCache::HIncrbyfloat(std::string &key, std::string &field, long double value)
{
    int ret;
    long double result;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    if (C_OK != (ret = RsHIncrbyfloat(m_RedisDB, kobj, fobj, value, &result))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, fobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj, fobj);
    return Status::OK();
}

Status
RedisCache::HLen(std::string &key, unsigned long *len)
{
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    if (C_OK != (ret = RsHlen(m_RedisDB, kobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj);
    return Status::OK();
}

Status
RedisCache::HStrlen(std::string &key, std::string &field, unsigned long *len)
{
    int ret;
    robj *kobj = createObject(OBJ_STRING, sdsnewlen(key.data(), key.size()));
    robj *fobj = createObject(OBJ_STRING, sdsnewlen(field.data(), field.size()));
    if (C_OK != (ret = RsHStrlen(m_RedisDB, kobj, fobj, len))) {
        if (REDIS_KEY_NOT_EXIST == ret) {
            DecrObjectsRefCount(kobj, fobj);
            return Status::NotFound("key not in cache");
        } else {
            DecrObjectsRefCount(kobj, fobj);
            return Status::Corruption("RsHGet failed");
        }
    }

    DecrObjectsRefCount(kobj, fobj);
    return Status::OK();
}

} // namespace dory

/* EOF */